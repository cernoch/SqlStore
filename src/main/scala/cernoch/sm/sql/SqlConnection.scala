package cernoch.sm.sql

import java.sql._
import cernoch.scalogic._


abstract class SqlConnection {

  def con: Connection

  def update(statement: String): Int = update(statement, List())
  def update(statement: String, arguments: List[Val[_]]): Int
  = prepare(statement, arguments).executeUpdate()

  def query(statement: String): ResultSet = query(statement, List())
  def query(statement: String, arguments: List[Val[_]]): ResultSet
  = prepare(statement, arguments).executeQuery()
  
  def execute(statement: String): Boolean = execute(statement, List())
  def execute(statement: String, arguments: List[Val[_]]): Boolean
  = prepare(statement, arguments).execute()

  protected def prepare
    (statement: String,
     arguments: List[Val[_]] = List())
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

  def transactionBegin = execute("BEGIN")
  def transactionCommit = execute("COMMIT")



  def nameTab(s: String) = ident(s.toUpperCase)
  def nameCol(s: String) = ident(s.toUpperCase)
  def nameIdx(s: String) = ident(s.toUpperCase)

  def ident(str: String)
  = if (str matches "[a-zA-Z][a-zA-Z0-9]*")
    str else "\"" + str.replaceAll("\"", "\\\"") + "\""

  def quote(str: String)
  = if (str matches "[a-zA-Z][a-zA-Z0-9]*")
    str else "`" + str.replaceAll("`", "``") + "`"



  def columnType
    (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_,_) => "NUMERIC"
    case CatDom(_,true,_) => "VARCHAR(250)"
    case CatDom(_,false,_) => "TEXT"
  }
}


trait PhysicalConncetionCache {
  
  protected def initConnection: Connection
  
  private var connection: Option[Connection] = None
  
  def con
  = connection.getOrElse{
    connection = Some(initConnection)
    connection.get
  }
}


class PostgresConnection(
    val host: String = "localhost",
    val port: Int = 5432,
    val user: String,
    val pass: String,
    val dtbs: String,
    val pfix: String = "" )
  extends SqlConnection
    with PhysicalConncetionCache {

  Class.forName("org.postgresql.Driver").newInstance()

  override def nameTab(s: String) = ident(pfix + s.toUpperCase)

  def initConnection = DriverManager.getConnection(
    "jdbc:postgresql://" + host + ":" + port + "/" + dtbs, user, pass)
}


class MysqlConnection(
    val host: String = "localhost",
    val port: Int = 3306,
    val user: String,
    val pass: String,
    val dtbs: String,
    val pfix: String = "" )
  extends SqlConnection
  with PhysicalConncetionCache {

  override def nameTab(s: String)
  = quote(pfix + s.toUpperCase)

  override def nameCol(s: String)
  = quote(pfix + s.toUpperCase)

  override def nameIdx(s: String)
  = s.toLowerCase
    .replaceAll("[^a-zA-Z0-9]","")
    .replaceAll("-","_")

  def initConnection = DriverManager.getConnection(
    "jdbc:mysql://" + host + ":" + port + "/" + dtbs +
      List("useUnicode=yes",
        "characterEncoding=UTF-8",
        "connectionCollation=utf8_general_ci"
      ).mkString("?", "&amp;", ""),
    user, pass)

  override def columnType
    (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_,_) => "NUMERIC"
    case CatDom(_,_,_) => "VARCHAR(250)"
  }
}








class DerbyMemoryConnection extends SqlConnection {

  Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
  private val url = "jdbc:derby:memory:sm;create=true"

  override def con()
  = DriverManager.getConnection(url)

  override def transactionBegin = true
  override def transactionCommit = true


  override def columnType
    (d: Domain[_])
  = d match {
    case DecDom(_) => "DOUBLE PRECISION"
    case NumDom(_,_) => "BIGINT"
    case CatDom(_,_,_) => "VARCHAR(250)"
  }
}



trait LoggingInterceptor extends SqlConnection {

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