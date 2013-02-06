package cernoch.sm.sql.jdbc

import java.sql.DriverManager
import cernoch.scalogic.{CatDom, NumDom, DecDom, Domain}

/**
 * Creates an in-memory Derby database
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class DerbyMemAdaptor(db: String) extends JDBCAdaptor {
  Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
  private val url = "jdbc:derby:memory:"+db+";create=true"

  override def con()
  = DriverManager.getConnection(url)

  override def columnDefinition
    (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_,_) => "BIGINT"
    case CatDom(_,_,_) => "VARCHAR(250)"
  }
}
