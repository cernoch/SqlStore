package cernoch.scalogic.sql

import java.sql.DriverManager
import cernoch.scalogic.Domain

class MySQLAdaptor
	(val host: String = "localhost",
	 val port: Int = 3306,
	 val user: String,
	 val pass: String,
	 val base: String,
	 val pfix: String = "")
	extends Adaptor {

	override def escapeTable(s: String) = Tools.grave(pfix + s)

	override def escapeColumn(s: String) = Tools.grave(pfix + s)

	override def escapeIndex(t: String, c: String)
	= super.escapeIndex(t,c).toLowerCase
		.replaceAll("[^a-zA-Z0-9]","")
		.replaceAll("-","_")

	protected val url
	= "jdbc:mysql://" + host + ":" + port + "/" + base +
		List("useUnicode=yes",
			"characterEncoding=UTF-8",
			"connectionCollation=utf8_general_ci"
		).mkString("?", "&amp;", "")

	Class.forName("com.mysql.jdbc.Driver").newInstance()
	def createCon = DriverManager.getConnection(url,user, pass)
}

class PostgresAdaptor
	(val host: String = "localhost",
	 val port: Int = 5432,
	 val user: String,
	 val pass: String,
	 val base: String,
	 val pfix: String = "")
	extends Adaptor {

	override def escapeTable(s: String) = Tools.quote(pfix + s)
	override def escapeColumn(s: String) = Tools.quote(pfix + s)
	override def escapeIndex(t: String, c: String) = Tools.quote(pfix + t + "_" + c)

	protected val url = "jdbc:postgresql://" + host + ":" + port + "/" + base
	Class.forName("org.postgresql.Driver").newInstance()
	def createCon = DriverManager.getConnection(url, user, pass)
}

/**
 * Creates an in-memory Derby database
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class DerbyMemAdaptor(db: String) extends Adaptor {

	protected val url = "jdbc:derby:memory:"+db+";create=true"
	Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
	def createCon = DriverManager.getConnection(url)

	override def columnDefinition(d: Domain)
	= d match {
		case _:Fractional[_] => "DOUBLE PRECISION"
		case _:Numeric[_] => "BIGINT"
		case _ => "VARCHAR(250)"
	}
}
