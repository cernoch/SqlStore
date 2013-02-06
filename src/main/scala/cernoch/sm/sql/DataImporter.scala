package cernoch.sm.sql

import exceptions.BackendError
import jdbc.JDBCAdaptor
import cernoch.scalogic.{Var, Btom, Val, Atom}

/**
 * Imports data into the database
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class DataImporter private[sql]
    (ada: JDBCAdaptor, schema: List[Btom[Var]])
    extends IsEnabled {

  private val atom2btom = new Atom2Btom(schema)
  private val names = new SchemaNames(ada, schema)
  import names._

  /**
   * Imports a single clause into the database
   *
   * This method is only callable between [[cernoch.sm.sql.SqlStorage.reset]]
   * and [[cernoch.sm.sql.DataImporter.close]]. Calling this method
   * inbetween will throw an error.
   *
   * Since the SQL databases work with relational calculus, no variables,
   * nor function symbols are allowed. Hence only {{{Atom[Val[_]]}}}
   * is allowed.
   *
   * As SQL is not a deductive database, clauses mustn't have bodies.
   */
  def put(cl: Atom[Val[_]]) { onlyIfEnabled {

    // Find all atoms in the schema that match the imported clause.
    val btom = atom2btom(cl)

    // Create a dummy query
    val sql = "INSERT INTO " + table(btom) +
      btom.args.map{ _ => "?" }.mkString(" VALUES ( ", ", ", " )")

    // And execute!
    ada.update(sql, cl.args) match {
      case 1 =>
      case _ => throw new BackendError("Clause was not added: " + cl)
    }
  }}

  def close() { tryClose(ada.close) }

  def done() = { tryClose {
    schema.foreach( btom => { btom.args.map(bvar => {
      "CREATE INDEX " + idx(btom)(bvar) + " ON " + table(btom) + " (" + col(btom)(bvar) + ")"
    }).foreach(ada.execute) })
    new QueryExecutor(ada,  schema)
  }}
}

