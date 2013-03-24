package cernoch.scalogic.sql

import cernoch.scalogic._
import grizzled.slf4j.Logging

/**
 * Connects ScaLogic to the SQL world.
 *
 * Allows data import and querying.
 */
class SqlStorage(ada: Adaptor, sch: List[Atom])
	extends IsEnabled with Logging { storage =>

	private val names = new Archetypes(ada, sch);import names._

	/**
	 * Opens the connection to an already-created database
	 */
	def open = tryClose {new SqlExecutor(ada, names)}

	/**
	 * Removes all tables from the database and recreates its structure
	 */
	def reset() = tryClose {
		ada.withConnection(con => {
			sch.view // We need not to store the result
				.map (aom2esc) // Get SQL-escaped identifier
				.map { "DROP TABLE " + _ }
				.foreach(sql => {
					debug(s"Trying to drop a table\n$sql")
					try { ada.execute(con, sql) } // DROPPING THE TABLE! BEWARE!!!
					catch { case t: Throwable => debug("Table not dropped.",t) }
					// Ignore errors (hopefully they all are "table does not exist")
				})

			sch.view
				.map(btom => {
					"CREATE TABLE " + aom2esc(btom) +
					btom.vars
						.map(v => { avr2esc(btom)(v) + " " + ada.columnDefinition(v.dom)})
						.mkString(" ( ", " , ", " )")
				}).foreach(sql => {
					debug(s"Creating a new table\n$sql")
					ada.execute(con,sql)
				})
		})

		new SqlImporter(ada, sch, names)
	}
}
