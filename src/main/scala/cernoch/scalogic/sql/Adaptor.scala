package cernoch.scalogic.sql

import java.sql._
import Types._
import cernoch.scalogic._
import math.{BigInt, BigDecimal => BigDec}
import java.math.{BigInteger, BigDecimal}
import grizzled.slf4j.Logging


/**
 * Encapsulates a JDBC connection and defines the SQL dialect
 */
abstract class Adaptor extends Logging {

	/**
	 * Create a new JDBC connection
	 *
	 * WARNING: The caller is responsible for closing the connection!
	 */
	protected def createCon: Connection

	/**
	 * Executes a handler, which takes an active connection
	 *
	 * @param handler Inside the handler, the connection is alive
	 * @tparam T Result type of the handler
	 */
	def withConnection[T]
	(handler: Connection => T)
	= {
		debug("Creating a new connection.")
		val connection = createCon
		try { handler(connection) }
		finally {
			connection.close()
			debug("Connection has been closed.")
		}
	}


	def update
	(con: Connection, sql: String,
	 arg: List[Val] = List())
	= {
		trace(s"Update query called.\n$sql")
		val statement = prepare(con,sql,arg)
		try { statement.executeUpdate() }
		finally {
			statement.close()
			trace(s"Statement closed.")
		}
	}

	def query[T]
	(con: Connection, sql: String,
	 arg: List[Val] = List(),
	 handler: ResultSet => T)
	= {
		trace(s"Executing query.\n$sql")
		val query = prepare(con, sql, arg).executeQuery()
		try { handler(query) }
		finally { query.close()
			trace(s"Query closed.")
		}
	}

	def execute
	(con: Connection, sql: String,
	 arguments: List[Val] = List()): Boolean
	= {
		trace(s"Executing statement.\n$sql")
		val statement = prepare(con, sql, arguments)
		try { statement.execute() }
		finally {
			statement.close()
			trace(s"Statement closed.")
		}
	}

	protected def prepare
	(con: Connection,
	 sql: String, arg: List[Val] = List())
	: PreparedStatement
	= {
		trace(s"Preparing statement.\n$sql")
		val statement = con.prepareStatement(sql)

		for ((arg,pos) <- arg zip Stream.from(1)) {
			trace(s"Injecting value $arg into the query at pos $pos.")
			injectArgument(statement, pos, arg)
		}
		statement
	}

	/**
	 * Converts a value befere passing to JDBC
	 *
	 * By default, this is an identity function. Override it
	 * to implement a custom value mapper.
	 */
	protected def convIntoSQL[T]
	(scalogicObject: T, domain: Domain)
	= scalogicObject

	/**
	 * Injects a [[cernoch.scalogic.Val]] into a SQL query.
	 */
	protected def injectArgument
	(sql: PreparedStatement,
	 pos: Int, arg: Val) { arg match {

		case DecVal(v:BigDec,f)
		=> Option(convIntoSQL(v, arg.dom)) match {
			case None       => sql.setNull(      pos, DECIMAL)
			case Some(some) => sql.setBigDecimal(pos, some.bigDecimal)
		}

		case DecVal(v,f)
		=> Option(convIntoSQL(f.toDouble(v), arg.dom)) match {
			case None       => sql.setNull(  pos, DOUBLE)
			case Some(some) => sql.setDouble(pos, some)
		}

		case NumVal(v:BigInt,n)
		=> Option(convIntoSQL(v, arg.dom)) match {
			case None       => sql.setNull(  pos, BIGINT)
			case Some(some) => sql.setString(pos, some.bigInteger.toString)
		}

		case NumVal(v,n)
		=> Option(convIntoSQL(n.toLong(v), arg.dom)) match {
			case None       => sql.setNull(pos, INTEGER)
			case Some(some) => sql.setLong(pos, some)
		}

		case StrVal(v,i)
		=> Option(convIntoSQL(v, arg.dom)) match {
			case None       => sql.setNull(  pos, VARCHAR)
			case Some(some) => sql.setString(pos, some)
		}
	}}

	/**
	 * Converts a value obtained from JDBC
	 *
	 * By default, this is an identity function. Override it
	 * to implement a custom value mapper.
	 */
	protected def convFromSQL[T]
	(scalogicObject: T, domain: Domain)
	= scalogicObject

	private def string2BigInt(s: String)
	= s match {
		case null => null
		case some => BigInt(some)
	}

	private def bigDecimal2dec(b: BigDecimal)
	= b match {
		case null => null
		case some => BigDec(some)
	}

	/**
	 * Extracts [[cernoch.scalogic.Val]] from a SQL query result.
	 */
	def extractArgument
	(result: ResultSet,
	 column: String,
	 domain: Domain)
	: Val
	= {
		trace(s"Extracting value from column '$column' using '$domain'.")

		domain match {
			case IntDom(_,d)  => Val(convFromSQL(result.getInt(column),d),d)
			case LongDom(_,d)  => Val(convFromSQL(result.getLong(column),d),d)
			case BigIntDom(_,d) => Val(convFromSQL(string2BigInt(
				result.getString(column) ),d),d)

			case FloatDom(_,d) => Val(convFromSQL(result.getFloat(column),d),d)
			case DoubleDom(_,d) => Val(convFromSQL(result.getDouble(column),d),d)
			case BigDecDom(_,d)  => Val(convFromSQL(bigDecimal2dec(
				result.getBigDecimal(column) ),d),d)

			case _ => Val(result.getString(column), domain)
		}
	}

	/** Converts a table name into SQL-insertable string */
	def escapeTable(s: String) = Tools.quote(s)

	/** Converts a column name into SQL-insertable string */
	def escapeColumn(s: String) = Tools.quote(s)

	/** Converts a table and column name into SQL-insertable index name */
	def escapeIndex(t: String, c: String) = Tools.quote(t + "_" + c)

	/** Defines the name of SQL column in the schema based on the domain */
	def columnDefinition(d: Domain) = d match {
		case _:Fractional[_] => "DOUBLE PRECISION"
		case _:Numeric[_] => "NUMERIC"
		case _ => "VARCHAR(250)"
	}
}



/**
 * Keeps the connection in the cache
 *
 * If there is any exception when the connection is in use, the
 * connection is closed and a new one is created on the next request.
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
trait ConnectionCache
	extends Adaptor {

	protected var cache
	: Option[Connection] = None

	override def withConnection[T]
	(handler: Connection => T)
	= {
		val connection = cache.getOrElse {
			debug("No connection in the cache. Creating a new one.")
			cache = Some(createCon)
			cache.get
		}
		try   { handler(connection) }
		catch { case e: Throwable => {
			cache = None
			info("Handler failed. Cache emptied, closing connection, rethrowing.")
			try   { connection.close() }
			catch { case e: Throwable => debug("Error while closing connection.",e) }
			throw e
		}}
	}

	def close {
		cache.foreach(_.close())
		cache = None
	}
}



/**
 * Adaptor that resets itself every N queries
 */
trait ResettingCache extends ConnectionCache {

	/**
	 * Number of [[cernoch.scalogic.sql.Adaptor.withConnection]] calls
	 * that happened using the current connection.
	 */
	protected var usageCounter = 0

	/**
	 * Maximum number of [[cernoch.scalogic.sql.Adaptor.withConnection]]
	 * calls before the connection cache is flushed.
	 */
	protected var usageLimit = 1000

	override def withConnection[T](f: Connection => T)
	= {
		usageCounter = usageCounter + 1
		trace(s"Connection requested for $usageCounter-th time.")
		if (usageCounter > usageLimit) {
			debug(s"Resetting limit reached. Resetting connection.")
			usageCounter = 0
			close
		}
		super.withConnection(f)
	}
}



/**
 * Logs every query as ''INFO''.
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
trait QueryLogger extends Adaptor {

	protected def handle(s: String) {
		info(s"Executing query:\n$s")
	}

	override protected def prepare
	(con: Connection,
	 sql: String,
	 arg: List[Val])
	= {
		handle(
			arg.view.map{v => v.value match {
				case null => "NULL"
				case args => "'" + v.value.toString.replaceAll("'", "\\'") + "'"
			}}.foldLeft(sql){_.replaceFirst("\\?", _)}
		)
		super.prepare(con,sql,arg)
	}
}
