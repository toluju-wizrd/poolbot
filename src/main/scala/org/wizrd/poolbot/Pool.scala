package org.wizrd.poolbot

import org.wizrd.util.Logging

import scala.collection.mutable.HashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class Pool[T](val factory:ConnectionFactory[T],
              val minPool:Int = 3, val maxPool:Int = 3,
              val keepAlive:Long = 10000, val maxErrors:Int = 3,
              val maxLoanTime:Long = 10000, val maxRetries:Int = 3)
              extends Logging {
  private val queue = new LinkedBlockingQueue[T]
  private val connInfos = new HashMap[T, ConnInfo]
  private var isClosed = false

  for (i <- 0 until minPool) queue.put(addConn)

  private val worker = new Thread() {
    override def run() = {
      while (isClosed == false) {
        if (queue.isEmpty) reclaimConn match {
          case Some(c) => queue.put(c)
          case None => if (connInfos.size < maxPool) queue.put(addConn)
        }
        else if (queue.size > minPool) {
          connInfos.find(_._2.age > keepAlive).foreach(c => {
            log.info("Connection reached maximum age")
            queue.remove(c._1)
            removeConn(c._1)
          })
        }

        Thread.sleep(500)
      }
    }
  }

  worker.setDaemon(true)
  worker.start

  private def addConn:T = {
    log.info("Adding connection")
    val conn = factory.create
    connInfos.put(conn, new ConnInfo)
    conn
  }

  private def removeConn(conn:T) = {
    log.info("Removing connection")
    connInfos.remove(conn)
    factory.close(conn)
  }

  private def reclaimConn:Option[T] = {
    connInfos.find(_._2.loanTime > maxLoanTime).map(conn => {
      log.warn("Reclaiming connection that has been loaned for too long")
      conn._1
    })
  }

  private def takeConn(timeout:Long) = {
    if (timeout < 0) Some(queue.take)
    else Option(queue.poll(timeout, TimeUnit.MILLISECONDS))
  }

  def take(timeout:Long = -1l):Option[T] = if (isClosed) None else {
    takeConn(timeout).map(c => {
      log.debug("Took connection %s", c)
      connInfos(c).updateLoaned
      c
    })
  }

  def give(conn:T) = if (!isClosed) {
    if (connInfos(conn).numErrors > maxErrors) {
      log.warn("Removing connection, too many errors")
      removeConn(conn)
    }
    else {
      queue.put(conn)
    }

    log.debug("Returning connection %s, queue size: %d", conn, queue.size)
  }

  def apply[R](fun: T => R):R = {
    retry(0, fun)
  }

  private def retry[R](count:Int, fun: T => R):R = {
    val conn = take().get
    try {
      val r = fun(conn)
      give(conn)
      r
    }
    catch { case e:Exception =>
      if (count > maxRetries) {
        log.warn("Retries exhausted")
        throw e
      }
      else {
        connInfos(conn).numErrors += 1
        give(conn)
        retry(count + 1, fun)
      }
    }
  }

  def close = {
    isClosed = true
    queue.clear
    connInfos.keys.foreach(c => factory.close(c))
    connInfos.clear
  }
}

private[poolbot] class ConnInfo {
  val created = System.currentTimeMillis
  def age = System.currentTimeMillis - created
  var numErrors = 0
  private var loanedAt = 0l
  def updateLoaned = loanedAt = System.currentTimeMillis
  def loanTime = System.currentTimeMillis - loanedAt
  override def toString = "age=%d,numErrors=%d,loanTime=%d" format (age, numErrors, loanTime)
}

trait ConnectionFactory[T] {
  def create():T
  def close(conn:T):Unit
}
