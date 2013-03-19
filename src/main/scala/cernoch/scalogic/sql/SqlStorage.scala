package cernoch.scalogic.sql

import cernoch.scalogic._

/**
 * Connects ScaLogic to the SQL world.
 *
 * Allows data import and querying.
 */
class SqlStorage(ada: Adaptor, sch: List[Atom])
	extends IsEnabled { storage =>

	private val names = new ArchetypeNames(ada, sch);import names._

	/**
	 * Opens the connection to an already-created database
	 */
	def open = tryClose {new SqlExecutor(ada, sch)}

	/**
	 * Removes all tables from the database and recreates its structure
	 */
	def reset() = { tryClose {
		ada.withConnection(con => {
			sch.view // We need not to store the result
				.map (aom2esc) // Get SQL-escaped identifier
				.map { "DROP TABLE " + _ }
				.foreach { sql =>
					try { ada.execute(con, sql) } // DROPPING THE TABLE! BEWARE!!!
					catch { case _: Throwable => } // Ignore errors (hopefully they all are "table does not exist")
				}

			sch.view
				.map(btom => {
				"CREATE TABLE " + aom2esc(btom) +
					btom.vars
						.map(v => { avr2esc(btom)(v) + " " + ada.columnDefinition(v.dom)})
						.mkString(" ( ", " , ", " )")
			}).foreach{sql => ada.execute(con,sql)}
		})

		new SqlImporter(ada, sch)
	}}
}
