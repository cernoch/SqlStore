package cernoch.sm.sql

import cernoch.scalogic.Val
import jdbc.JDBCAdaptor
import java.sql.Connection

/**
 * Prints every query to stdout
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
trait QueryLogger extends JDBCAdaptor {

  protected def handle(s: String) = println(s + ";")

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
