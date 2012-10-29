package cernoch.sm.sql

import java.sql.{DriverManager, Connection}
import cernoch.scalogic.{CatDom, NumDom, DecDom, Domain}


class SqlSettings extends (() => Connection) {
  var driver: String = System.getProperty("sm.sql.jdbc.driver", "postgresql")
  var clName: String = System.getProperty("sm.sql.jdbc.clName", "org.postgresql.Driver")

  var host: String = System.getProperty("sm.sql.host", "localhost")
  var port: String = System.getProperty("sm.sql.port", "5432")
  var username: String = System.getProperty("sm.sql.username", "sss")
  var password: String = System.getProperty("sm.sql.password", "sss")
  var database: String = System.getProperty("sm.sql.database", "sss")
  var prefix: String = System.getProperty("sm.sql.prefix", "")

  def connUrl = List("jdbc", driver, database).reduce(_ + ":" + _)

  def transactions = false

  def domainName
    (d: Domain[_])
  = d match {
      case DecDom(_) => "double precision"
      case NumDom(_,_) => "numeric"
      case CatDom(_,_,_) => "text"
    }

  def apply = {
    Class.forName(clName);
    DriverManager.getConnection(
      connUrl, username, password)
  }
}
