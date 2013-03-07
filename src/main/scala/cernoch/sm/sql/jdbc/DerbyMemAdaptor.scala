package cernoch.sm.sql.jdbc

import cernoch.scalogic._
import java.sql.DriverManager

/**
 * Creates an in-memory Derby database
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class DerbyMemAdaptor(db: String) extends JDBCAdaptor {
  Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
  private val url = "jdbc:derby:memory:"+db+";create=true"

  def createCon = DriverManager.getConnection(url)

	override def columnDefinition(d: Domain)
	= d match {
		case _:Fractional[_] => "DOUBLE PRECISION"
		case _:Numeric[_] => "BIGINT"
		case _ => "VARCHAR(250)"
	}
}
