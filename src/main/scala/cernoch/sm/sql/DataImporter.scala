package cernoch.sm.sql

import exceptions.BackendError
import jdbc.JDBCAdaptor
import cernoch.scalogic._

/**
 * Imports data into the database
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class DataImporter private[sql]
    (ada: JDBCAdaptor, schema: List[Mode])
    extends IsEnabled {

  private val atom2btom = new Atom2Btom(schema)
  private val names = new SchemaNames(ada, schema)
  import names._

  /**
   * Imports a single clause into the database
   *
   * This method is only callable between
	 * [[cernoch.sm.sql.SqlStorage.reset]]
   * and [[cernoch.sm.sql.DataImporter]].
	 * Calling this method inbetween will throw an error.
   *
   * Since the SQL databases work with relational calculus, no variables,
   * nor function symbols are allowed. Hence only {{{Atom[Val[_]]}}}
   * is allowed.
   *
   * As SQL is not a deductive database, clauses mustn't have bodies.
   */
  def put(atom: Atom) { onlyIfEnabled {

    // Create a dummy query
    val sql = "INSERT INTO " + table(atom2btom(atom)) +
      atom.args.map{ _ => "?" }.mkString(" VALUES ( ", ", ", " )")

		val valArgs = atom.args.map{_ match {
			case v: Val => v
			case _ => throw new IllegalArgumentException(
				"Imported atom must only contain Values"
			)
		}}

    // And execute!
		ada.withConnection(con => {
			ada.update(con, sql, valArgs) match {
				case 1 =>
				case _ => throw new BackendError("Clause was not added: " + valArgs)
			}
		})
  }}

	def done() = tryClose {
		ada.withConnection(con => {
			schema.foreach( btom => {
				btom.vars.map(bVar =>
						"CREATE INDEX " + idx(btom)(bVar) +
						         " ON " + table(btom) +
						           " (" + col(btom)(bVar) + ")"
				).foreach(sql => ada.execute(con,sql)) })
		})
    new QueryExecutor(ada,schema)
  }
}