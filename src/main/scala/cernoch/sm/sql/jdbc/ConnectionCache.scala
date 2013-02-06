package cernoch.sm.sql.jdbc

import java.sql.Connection

/**
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
protected[sm] trait ConnectionCache {

  protected def initConnection: Connection

  protected var connection: Option[Connection] = None

  def resetConnection() {
    con().close()
    connection = None
  }

  def con() = {
    connection.getOrElse {
      connection = Some(initConnection)
      connection.get
    }
  }
}
