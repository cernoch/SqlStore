package cernoch.sm.sql.jdbc

import java.sql.DriverManager
import cernoch.scalogic.{CatDom, NumDom, DecDom, Domain}

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
  with ConnectionCache {

  override def escapeTable(s: String)
  = quote(pfix + s)

  override def escapeColumn(s: String)
  = quote(pfix + s)

  override def escapeIndex(t: String, c: String)
  = super.escapeIndex(t,c).toLowerCase
    .replaceAll("[^a-zA-Z0-9]","")
    .replaceAll("-","_")

  def initConnection
  = DriverManager.getConnection(
    "jdbc:mysql://" + host + ":" + port + "/" + dtbs +
      List("useUnicode=yes",
        "characterEncoding=UTF-8",
        "connectionCollation=utf8_general_ci"
      ).mkString("?", "&amp;", ""),
    user, pass)


  /**
   * Number of [[cernoch.sm.sql.JDBCAdaptor.con]] calls
   * with the current connection
   */
  protected var resetter = 0

  override def con = {
    resetter = resetter + 1
    if (resetter > 20000) {
      resetConnection
      resetter = 0
    }
    super.con
  }


  override def columnDefinition
  (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_,_) => "NUMERIC"
    case CatDom(_,_,_) => "VARCHAR(250)"
  }
}
