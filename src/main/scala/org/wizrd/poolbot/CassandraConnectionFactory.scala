package org.wizrd.poolbot

import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket

class CassandraConnectionFactory(val hosts:Iterable[String], val keyspace:String)
      extends ConnectionFactory[Cassandra.Client] {
  assert(hosts.size > 0)
  private var iterator = hosts.iterator

  def create() = {
    if (!iterator.hasNext) {
      iterator = hosts.iterator
    }

    val transport = new TFramedTransport(new TSocket(iterator.next, 9160))
    val protocol = new TBinaryProtocol(transport)
    val client = new Cassandra.Client(protocol)
    transport.open
    client.set_keyspace(keyspace)
    client
  }

  // not sure if this is necessary, but let's keep it for now
  def close(client:Cassandra.Client) = {
    if (client.getInputProtocol.getTransport.isOpen)
      client.getInputProtocol.getTransport.close
    if (client.getOutputProtocol.getTransport.isOpen)
      client.getOutputProtocol.getTransport.close
  }
}
