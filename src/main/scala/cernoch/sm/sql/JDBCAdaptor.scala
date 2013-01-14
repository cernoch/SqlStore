package cernoch.sm.sql

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
    case Num(data) => sql.setInt(   pos, data.toInt)
    case Dec(data) => sql.setDouble(pos, data.toDouble)
  }
  
  def extractArgument
    (result: ResultSet,
     column: String,
     domain: Domain[_])
  : Val[_]
  = domain match {
    case d:DecDom => new Dec(BigDecimal(result.getDouble(column)), d)
    case d:NumDom => new Num(BigInt(result.getInt(column)), d)
    case d:CatDom => new Cat(result.getString(column), d)
    case _ => throw new Exception("Internal error.")
  }

  def escTab(s: String) = ident(s.toUpperCase)
  def escapeColumn(s: String) = ident(s.toUpperCase)
  def escapeIndex(s: String) = ident(s.toUpperCase)

  protected def ident(str: String)
  = if (str matches "[a-zA-Z][a-zA-Z0-9]*")
    str else "\"" + str.replaceAll("\"", "\\\"") + "\""

  protected def quote(str: String)
  = if (str matches "[a-zA-Z][a-zA-Z0-9]*")
    str else "`" + str.replaceAll("`", "``") + "`"



  def columnDefinition
    (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_,_) => "NUMERIC"
    case CatDom(_,true,_) => "VARCHAR(250)"
    case CatDom(_,false,_) => "TEXT"
  }
}


trait ConncetionCache {
  
  protected def initConnection: Connection
  
  protected var connection: Option[Connection] = None

  def resetConnection
  = {
    con.close()
    connection = None
  }

  def con
  = connection.getOrElse{
    connection = Some(initConnection)
    connection.get
  }
}


class PostgresAdaptor(
    val host: String = "localhost",
    val port: Int = 5432,
    val user: String,
    val pass: String,
    val dtbs: String,
    val pfix: String = "" )
  extends JDBCAdaptor
    with ConncetionCache {

  Class.forName("org.postgresql.Driver").newInstance()

  override def escTab(s: String) = ident(pfix + s.toUpperCase)

  def initConnection = DriverManager.getConnection(
    "jdbc:postgresql://" + host + ":" + port + "/" + dtbs, user, pass)
}


class MySQLAdaptor(
    val host: String = "localhost",
    val port: Int = 3306,
    val user: String,
    val pass: String,
    val dtbs: String,
    val pfix: String = "" )
  extends JDBCAdaptor
  with ConncetionCache {

  override def escTab(s: String)
  = quote(pfix + s.toUpperCase)

  override def escapeColumn(s: String)
  = quote(pfix + s.toUpperCase)

  override def escapeIndex(s: String)
  = s.toLowerCase
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








class DerbyInMemory extends JDBCAdaptor {

  Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
  private val url = "jdbc:derby:memory:sm;create=true"

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



trait LoggingInterceptor extends JDBCAdaptor {

  protected def handle(s: String) = println(s + ";")

  override protected def prepare
    (statement: String,
     arguments: List[Val[_]] = List())
  = {
    handle(arguments.map{x =>
        if (x.get == null) "NULL"
        else x.get.toString.replaceAll("'", "\\'")
      }
      .map{"'" + _ + "'"}
      .foldLeft(statement){_.replaceFirst("\\?", _)}
    )
    super.prepare(statement, arguments)
  }
}