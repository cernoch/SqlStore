package cernoch.sm.sql.jdbc

import java.sql._
import cernoch.scalogic._

abstract class JDBCAdaptor {

  /**
   * JDBC connection
   */
  protected def createCon: Connection

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

		for ((arg,pos) <- arg zip Stream.from(1))
			setArgument(statement, pos, arg)

		statement
  }

	protected def setArgument
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
				BigInt(result.getLong(column)), // Can we do better?
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

  /**Given a raw table name, returns a string directly insertable into SQL comands */
  def escapeTable(s: String) = ident(s.toUpperCase)

  /**Given a raw column name, returns a string directly insertable into SQL comands */
  def escapeColumn(s: String) = ident(s.toUpperCase)

  /**Given a raw table and column name, returns a string directly insertable into SQL comands */
  def escapeIndex(t: String, c: String) = ident(t.toUpperCase + "_" + c.toUpperCase)

  protected def ident(str: String)
  = str match {
		case JDBCAdaptor.SimpleIdent() => str
		case _ => "\"" + str.replaceAll("\"", "\\\"") + "\""
	}

  protected def quote(str: String)
	= str match {
		case JDBCAdaptor.SimpleIdent() => str
		case _ => "`" + str.replaceAll("`", "``") + "`"
	}

  def columnDefinition(d: Domain)
  = d match {
    case _:Fractional[_] => "DOUBLE PRECISION"
    case _:Numeric[_] => "NUMERIC"
    case _ => "VARCHAR(250)"
  }
}

object JDBCAdaptor {
	protected val SimpleIdent = "[a-zA-Z][a-zA-Z0-9]*".r
}
