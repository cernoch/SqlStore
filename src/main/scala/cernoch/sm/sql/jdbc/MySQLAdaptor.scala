package cernoch.sm.sql.jdbc

import java.sql.DriverManager
import cernoch.scalogic._

/**
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class MySQLAdaptor(
                    val host: String = "localhost",
                    val port: Int = 3306,
                    val user: String,
                    val pass: String,
                    val dtbs: String,
                    val pfix: String = "" )
  extends JDBCAdaptor
  with ResettingCache {

  override def escapeTable(s: String)
  = quote(pfix + s)

  override def escapeColumn(s: String)
  = quote(pfix + s)

  override def escapeIndex(t: String, c: String)
  = super.escapeIndex(t,c).toLowerCase
    .replaceAll("[^a-zA-Z0-9]","")
    .replaceAll("-","_")

  def createCon
  = DriverManager.getConnection(
    "jdbc:mysql://" + host + ":" + port + "/" + dtbs +
      List("useUnicode=yes",
        "characterEncoding=UTF-8",
        "connectionCollation=utf8_general_ci"
      ).mkString("?", "&amp;", ""),
    user, pass)
}
