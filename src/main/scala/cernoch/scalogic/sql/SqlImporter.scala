package cernoch.scalogic.sql

import exceptions.BackendError
import cernoch.scalogic._
import tools.Labeler
import grizzled.slf4j.Logging

/**
 * Imports data into the database
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class SqlImporter private[sql]
	(ada: Adaptor,
	 sch: List[Atom],
	 aom2sql: Archetypes)
	extends IsEnabled
	with Logging {
	import aom2sql._

	/**
	 * Imports a single clause into the database
	 *
	 * This method is only callable between
	 * [[cernoch.scalogic.sql.SqlStorage.reset]]
	 * and [[cernoch.scalogic.sql.SqlImporter]].
	 * Calling this method inbetween will throw an error.
	 *
	 * Since the SQL databases work with relational calculus, no variables,
	 * nor function symbols are allowed. Hence only {{{Atom[Val[_]]}}}
	 * is allowed.
	 *
	 * As SQL is not a deductive database, clauses mustn't have bodies.
	 */
	def put(atom: Atom) { onlyIfEnabled {

		debug(s"Importing atom\n${atom.toString(false,Labeler.alphabet)}")

		// Create a dummy query
		val sql = "INSERT INTO " + aom2esc(aom2sql(atom)) +
			atom.args.map{ _ => "?" }.mkString(" VALUES ( ", ", ", " )")

		val valArgs = atom.args.map{_ match {
			case v: Val => v
			case _ => throw new IllegalArgumentException(
				"Imported atom must only contain Values" )
		}}

		debug(s"Translated into SQL query:\n$sql\n$valArgs")

		// And execute!
		ada.withConnection(con => {
			ada.update(con, sql, valArgs) match {
				case 1 =>
				case _ => throw new BackendError("Clause was not added: " + valArgs)
			}
		})
	}}

	def done() = tryClose {
		debug("Finishing the import, creating indexes.")
		ada.withConnection(con => {
			sch.foreach( btom => {
				btom.vars.map(bVar =>
					"CREATE INDEX " + idx2esc(btom)(bVar) +
						" ON " + aom2esc(btom) +
						" (" + avr2esc(btom)(bVar) + ")"
				).foreach(sql => {
					trace(s"Creating index using SQL\n$sql")
					ada.execute(con,sql)
				})
			})
		})
		new SqlExecutor(ada,aom2sql)
	}
}
