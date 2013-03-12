package cernoch.sm.sql.jdbc

import java.sql._
import cernoch.scalogic._
import cernoch.sm.sql.Tools

/**
 * Encapsulates a JDBC connection and defines the SQL dialect
 */
abstract class JDBCAdaptor {

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
		val result = statement.executeUpdate()
		statement.close()
		result
	}

	def query
	(con: Connection, sql: String,
	 arg: List[Val] = List())
	(handler: ResultSet => Unit)
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
		val result = statement.execute()
		statement.close()
		result
	}

	protected def prepare
	(con: Connection,
	 sql: String, arg: List[Val] = List())
	: PreparedStatement
	= {
		val statement = con.prepareStatement(sql)
		statement.setFetchSize(100)
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
		case cat:Cat[_] => if (cat.value == null)
			sql.setNull(pos, java.sql.Types.VARCHAR) else
			sql.setString(pos, cat.value.toString())

		case cat:Dec[_] => if (cat.value == null)
			sql.setNull(pos, java.sql.Types.DOUBLE) else
			sql.setDouble(pos, cat.dom.toDouble(cat.get.get))

		case cat:Num[_] => if (cat.value == null)
			sql.setNull(pos, java.sql.Types.INTEGER) else
			sql.setInt(pos, cat.dom.toInt(cat.get.get))

		case _ => throw new Exception("Unsupported value: " + arg)
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
