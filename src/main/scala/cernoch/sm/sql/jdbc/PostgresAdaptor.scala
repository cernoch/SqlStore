package cernoch.sm.sql.jdbc

import java.sql.DriverManager

/**
 *
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class PostgresAdaptor(
                       val host: String = "localhost",
                       val port: Int = 5432,
                       val user: String,
                       val pass: String,
                       val dtbs: String,
                       val pfix: String = "")
  extends JDBCAdaptor
  with ConnectionCache {

  Class.forName("org.postgresql.Driver").newInstance()

  override def escapeTable(s: String) = ident(pfix + s.toUpperCase)

  def initConnection = DriverManager.getConnection(
    "jdbc:postgresql://" + host + ":" + port + "/" + dtbs, user, pass)
}
