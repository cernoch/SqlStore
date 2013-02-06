package cernoch.sm.sql

import cernoch.scalogic.Val
import jdbc.JDBCAdaptor

/**
 * Prints every query to stdout
 */
trait QueryLogger extends JDBCAdaptor {

  protected def handle(s: String) = println(s + ";")

  override protected def prepare
  (statement: String,
   arguments: List[Val[_]] = List())
  = {
    handle(arguments.view
      .map {x => if (x.get == null) "NULL" else x.get.toString.replaceAll("'", "\\'") }
      .map {"'" + _ + "'"}
      .foldLeft(statement) {_.replaceFirst("\\?", _)}
    )
    super.prepare(statement, arguments)
  }
}
