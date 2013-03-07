package cernoch.sm.sql

import cernoch.scalogic._
import jdbc.JDBCAdaptor

/**
 * Maps ``scalogic`` to SQL database
 */
class SqlStorage(ada: JDBCAdaptor, schema: List[Mode])
    extends IsEnabled { storage =>

  private val names = new SchemaNames(ada, schema)

  /**
   * Opens the connection to an already-created database
   */
  def open = tryClose {new QueryExecutor(ada, schema)}

  /**
   * Removes all tables from the database and recreates its structure
   */
  def reset() = { tryClose {
		ada.withConnection(con => {
    schema.view // We need not to store the result
      .map (names.table) // Get SQL-escaped identifier
      .map { "DROP TABLE " + _ }
      .foreach { sql =>
        try { ada.execute(con, sql) } // DROPPING THE TABLE! BEWARE!!!
        catch { case _: Throwable => } // Ignore errors (hopefully they all are "table does not exist")
      }

    schema.view
      .map(btom => {
        "CREATE TABLE " + names.table(btom) +
          btom.vars
            .map(v => { names.col(btom)(v) + " " + ada.columnDefinition(v.dom)})
            .mkString(" ( ", " , ", " )")
      }).foreach{sql => ada.execute(con,sql)}
		})

    new DataImporter(ada, schema)
  }}
}
