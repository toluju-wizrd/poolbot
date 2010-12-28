package org.wizrd.poolbot

import java.sql.Connection
import java.sql.DriverManager

class JDBCConnectionFactory(val url:String, val user:String, val passwd:String)
      extends ConnectionFactory[Connection] {
  def create() = {
    DriverManager.getConnection(url, user, passwd)
  }

  def close(conn:Connection) = {
    conn.close
  }
}
