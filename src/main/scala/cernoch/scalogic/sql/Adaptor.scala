package cernoch.scalogic.sql

import java.sql._
import Types._
import cernoch.scalogic._
import math.{BigInt, BigDecimal => BigDec}


/**
 * Encapsulates a JDBC connection and defines the SQL dialect
 */
abstract class Adaptor {

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
		val connection = createCon
		try { handler(connection) }
		finally { connection.close() }
	}


	def update
	(con: Connection, sql: String,
	 arg: List[Val] = List())
	= {
		val statement = prepare(con, sql, arg)
		try { statement.executeUpdate() }
		finally { statement.close() }
	}

	def query[T]
	(con: Connection, sql: String,
	 arg: List[Val] = List(),
	 handler: ResultSet => T)
	= {
		val query = prepare(con, sql, arg).executeQuery()
		try { handler(query) }
		finally { query.close() }
	}

	def execute
	(con: Connection, sql: String,
	 arguments: List[Val] = List()): Boolean
	= {
		val statement = prepare(con, sql, arguments)
		try { statement.execute() }
		finally { statement.close() }
	}

	protected def prepare
	(con: Connection,
	 sql: String, arg: List[Val] = List())
	: PreparedStatement
	= {
		val statement = con.prepareStatement(sql)
		for ((arg,pos) <- arg zip Stream.from(1))
			injectArgument(statement, pos, arg)
		statement
	}

	/**
	 * Injects a [[cernoch.scalogic.Val]] into a SQL query.
	 */
	protected def injectArgument
	(sql: PreparedStatement,
	 pos: Int, arg: Val)
	= arg match {
		case DecVal(v:BigDec,f) => if (v == null)
			sql.setNull(pos, DECIMAL) else
			sql.setBigDecimal(pos, v.bigDecimal)

		case DecVal(v,f) => if (v == null)
			sql.setNull(pos, DOUBLE) else
			sql.setDouble(pos, f.toDouble(v))

		case NumVal(v:BigInt,n) => if (v == null)
			sql.setNull(pos, BIGINT) else
			sql.setString(pos, v.bigInteger.toString)

		case NumVal(v,n) => if (v == null)
			sql.setNull(pos, INTEGER) else
			sql.setLong(pos, n.toLong(v))

		case StrVal(v,i) => if (v == null)
			sql.setNull(pos, VARCHAR) else
			sql.setString(pos, v)
	}

	/**
	 * Extracts [[cernoch.scalogic.Val]] from a SQL query result.
	 */
	def extractArgument
	(result: ResultSet,
	 column: String,
	 domain: Domain)
	: Val
	= domain match {
		case int: Integral[_] => int.zero match  {
			case _:Int => Val(
				result.getInt(column),
				domain.asInstanceOf[Domain with Integral[Int]])

			case _:Long => Val(
				result.getLong(column),
				domain.asInstanceOf[Domain with Integral[Long]])

			case _:BigInt => Val(
				BigInt(result.getLong(column)), // TODO: Can we do better?
				domain.asInstanceOf[Domain with Integral[BigInt]])

			case _ => throw new Exception("Unsupported domain type: " + domain)
		}

		case frac: Fractional[_] => frac.zero match  {
			case _:Float => Val(
				result.getFloat(column),
				domain.asInstanceOf[Domain with Fractional[Float]])

			case _:Double => Val(
				result.getDouble(column),
				domain.asInstanceOf[Domain with Fractional[Double]])

			case _:BigDecimal => Val(
				result.getBigDecimal(column),
				domain.asInstanceOf[Domain with Fractional[BigDecimal]])

			case _ => throw new Exception("Unsupported domain type: " + domain)
		}

		case _ => Val(result.getString(column), domain)
	}

	/** Converts a table name into SQL-insertable string */
	def escapeTable(s: String) = Tools.quote(s)

	/** Converts a column name into SQL-insertable string */
	def escapeColumn(s: String) = Tools.quote(s)

	/** Converts a table and column name into SQL-insertable index name */
	def escapeIndex(t: String, c: String) = Tools.quote(t + "_" + c)

	/**
	 * Defines the name of SQL column in the schema based on the domain
	 */
	def columnDefinition(d: Domain) = d match {
		case _:Fractional[_] => "DOUBLE PRECISION"
		case _:Numeric[_] => "NUMERIC"
		case _ => "VARCHAR(250)"
	}
}



/**
 * Keeps the connection in the cache
 *
 * If there is any exception when the connection is in use,
 * the connection is closed and a new one is created on the
 * next request.
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
			cache = Some(createCon)
			cache.get
		}
		try   { handler(connection) }
		catch { case e: Throwable => {
			cache = None
			try   { connection.close() }
			catch { case e: Throwable => {} }
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
 *
 */
trait ResettingCache extends ConnectionCache {

	/**
	 * Number of [[Adaptor.withConnection]]
	 * calls with the current connection
	 */
	protected var usageCounter = 0

	protected var usageLimit = 1000

	override def withConnection[T](f: Connection => T)
	= {
		usageCounter = usageCounter + 1
		if (usageCounter > usageLimit) {
			usageCounter = 0
			close
		}
		super.withConnection(f)
	}
}



/**
 * Prints every query to stdout
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
trait QueryLogger extends Adaptor {

	protected def handle(s: String)
	= println(s + ";")

	override protected
	def prepare
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