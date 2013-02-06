package cernoch.sm.sql.jdbc

import java.sql._
import cernoch.scalogic._


abstract class JDBCAdaptor {

  def close = con.close

  /**
   * JDBC connection
   */
  protected def con: Connection

  def update(sql: String): Int = update(sql, List())

  def update(sql: String, arguments: List[Val[_]]): Int
  = {
    val statement = prepare(sql, arguments)
    val result = statement.executeUpdate()
    statement.close()
    result
  }

  def query(sql: String): ResultSet = query(sql, List())

  def query(sql: String, arguments: List[Val[_]]): ResultSet
  = prepare(sql, arguments).executeQuery()

  def execute(sql: String): Boolean = execute(sql, List())

  def execute(sql: String, arguments: List[Val[_]]): Boolean
  = {
    val statement = prepare(sql, arguments)
    val result = statement.execute()
    statement.close()
    result
  }

  protected def prepare
  (statement: String,
   arguments: List[Val[_]] = List())
  : PreparedStatement
  = {
    val sql = con.prepareStatement(statement)

    (arguments zip Stream.from(1)).foreach {
      case (arg, pos) => setArgument(sql, pos, arg)
    }
    sql
  }

  protected def setArgument
  (sql: PreparedStatement,
   pos: Int,
   arg: Val[_])
  = arg match {
    case Cat(null) => sql.setNull(pos, java.sql.Types.VARCHAR)
    case Num(null) => sql.setNull(pos, java.sql.Types.INTEGER)
    case Dec(null) => sql.setNull(pos, java.sql.Types.DOUBLE)
    case Cat(data) => sql.setString(pos, data)
    case Num(data) => sql.setInt(pos, data.toInt)
    case Dec(data) => sql.setDouble(pos, data.toDouble)
  }

  def extractArgument
  (result: ResultSet,
   column: String,
   domain: Domain[_])
  : Val[_]
  = domain match {
    case d: DecDom => new Dec(BigDecimal(result.getDouble(column)), d)
    case d: NumDom => new Num(BigInt(result.getInt(column)), d)
    case d: CatDom => new Cat(result.getString(column), d)
    case _ => throw new Exception("Internal error.")
  }

  /**Given a raw table name, returns a string directly insertable into SQL comands */
  def escapeTable(s: String) = ident(s.toUpperCase)

  /**Given a raw column name, returns a string directly insertable into SQL comands */
  def escapeColumn(s: String) = ident(s.toUpperCase)

  /**Given a raw table and column name, returns a string directly insertable into SQL comands */
  def escapeIndex(t: String, c: String) = ident(t.toUpperCase + "_" + c.toUpperCase)

  protected def ident(str: String)
  = if (str matches "[a-zA-Z][a-zA-Z0-9]*")
    str
  else "\"" + str.replaceAll("\"", "\\\"") + "\""

  protected def quote(str: String)
  = if (str matches "[a-zA-Z][a-zA-Z0-9]*")
    str
  else "`" + str.replaceAll("`", "``") + "`"


  def columnDefinition
  (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_, _) => "NUMERIC"
    case CatDom(_, true, _) => "VARCHAR(250)"
    case CatDom(_, false, _) => "TEXT"
  }
}



