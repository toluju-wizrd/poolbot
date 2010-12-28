package org.wizrd.poolbot

import org.wizrd.util.Logger
import org.wizrd.util.Logging

import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import scala.util.Random
import scala.actors.Actor
import scala.actors.Actor._

object JDBCExample extends Logging {
  def insert(i:Int)(conn:Connection) = {
    val stmt = conn.prepareStatement("insert into users (id, name) values (?, ?)")
    stmt.setInt(1, i)
    stmt.setString(2, UUID.randomUUID.toString)
    stmt.executeUpdate
    stmt.close
  }

  def select(id:Int)(conn:Connection) = {
    val stmt = conn.prepareStatement("select name from users where id=?")
    stmt.setInt(1, id)
    val rs = stmt.executeQuery
    if (rs.next) {
      log.trace("Name for id %d: %s", id, rs.getString("name"))
    }
    else {
      log.trace("Id not found: %d", id)
    }
    rs.close
    stmt.close
  }

  def main(args: Array[String]) {
    Logger.init()

    //standardTests
    badConnectionTests
  }

  def standardTests {
    val factory = new JDBCConnectionFactory("jdbc:mysql://localhost/poolbot_test", "poolbot", "poolbot")
    val pool = new Pool(factory, 2, 4, 3000, 5, 3000)

    log.info("Inserting values for keys 0-9")
    for (i <- 0 until 10) {
      pool {
        insert(i)
      }
    }

    log.info("Inserting duplicate key, expecting error")
    pool {
      insert(5)
    }

    log.info("Selecting 10 random rows")
    val random = new Random
    for (i <- 0 until 10) {
      pool {
        select(random.nextInt(10))
      }
    }

    log.info("Selecting non-existant row")
    pool {
      select(15)
    }

    log.info("Running 10 threads 50 times to force new connections to be spawned")
    runThreads(pool, 10, 50)

    log.info("Waiting 4 seconds to make sure the extra connections die")
    Thread.sleep(4000)

    log.info("Issuing two new requests to flush the expired connections")
    pool {
      select(5)
    }
    pool {
      select(2)
    }

    log.info("Taking a connection but not returning, then waiting")
    val c = pool.take()
    Thread.sleep(4000)

    log.info("Running 10 threads 10 times to steal the unreleased connection")
    runThreads(pool, 10, 10)

    log.info("Shutting down")
    pool.close
  }

  def runThreads(pool:Pool[Connection], count:Int, times:Int) = {
    val ts = (0 until count).map(i => {
      val t = new Thread() {
        val random = new Random
        override def run = {
          for (i <- 0 until times) {
            pool {
              select(random.nextInt(10))
            }
          }
        }
      }
      t.start
      t
    })
    ts.foreach(_.join)
  }

  def badConnectionTests = {
    val factory = new ConnectionFactory[Connection] {
      val urls = Seq("jdbc:mysql://localhost/poolbot_test", "jdbc:mysql://localhost/poolbot_test2")
      var toggle = false
      
      def create() = {
        val url = if (toggle) urls.head else urls.last
        toggle = !toggle
        DriverManager.getConnection(url, "poolbot", "poolbot")
      }

      def close(conn:Connection) = {
        conn.close
      }
    }

    val pool = new Pool(factory, 2, 2, 3000, 3, 3000, 3)

    log.info("Inserting values for keys 0-9")
    for (i <- 0 until 10) {
      pool {
        insert(i)
      }
    }

    log.info("Selecting 10 random rows")
    val random = new Random
    for (i <- 0 until 10) {
      pool {
        select(random.nextInt(10))
      }
    }

    pool.close
  }
}
