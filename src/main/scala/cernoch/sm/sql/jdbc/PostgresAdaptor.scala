package cernoch.sm.sql.jdbc

import java.sql.DriverManager
import cernoch.sm.sql.Tools

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
	val prefix: String)
  extends JDBCAdaptor {

  Class.forName("org.postgresql.Driver").newInstance()

  override def escapeTable(s: String) = Tools.quote(prefix + s)
  override def escapeColumn(s: String) = Tools.quote(prefix + s)
  override def escapeIndex(t: String, c: String) = Tools.quote(prefix + t + "_" + c)

  def createCon = DriverManager.getConnection(
    "jdbc:postgresql://" + host + ":" + port + "/" + dtbs, user, pass)
}
