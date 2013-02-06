package cernoch.sm.sql

import cernoch.sm.sql.Tools._
import cernoch.scalogic.{Var, Btom}
import jdbc.JDBCAdaptor

/**
 * Every object in the FOL world is mapped to a name in the SQL world
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
private class SchemaNames(ada: JDBCAdaptor, schema: List[Btom[Var]]) {

  /** Assign each atom in the schema a unique table name */
  val _tables = name (schema map {_.head}) {_.head.pred}
  /** Assign each column in a table a name */
  val _cols = schema.map{ btom =>
    btom.head -> name(btom.head.args){_.dom.name}
  }.toMap

  /**  Escaped table name to be used directly in SQL */
  def table(btom: Btom[Var]) = ada.escapeTable(_tables(btom))
  /** Escaped column name to be used directly in SQL */
  def col(btom: Btom[Var])(bvar: Var)
  = ada.escapeColumn(_cols(btom)(bvar))
  /** Escaped index name to be used directly in SQL */
  def idx(btom: Btom[Var])(bvar: Var)
  = ada escapeIndex(_tables(btom), _cols(btom)(bvar))
}
