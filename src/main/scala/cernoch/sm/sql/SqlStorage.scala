package cernoch.sm.sql

import cernoch.scalogic._
import exceptions.{BackendError, SchemaMismash}
import java.sql.ResultSet
import collection.mutable.ArrayBuffer
import jdbc.{JDBCAdaptor, JDBCResConvert}
import numeric._
import tools.StringUtils.mkStringIfNonEmpty
import Tools._

class SqlStorage(ada: JDBCAdaptor, schema: List[Btom[Var]]) extends IsEnabled { storage =>

  /**
   * Fast index for mapping arbitrary atoms to their representative btoms.
   *
   * Note: Btom in this context is a “prototype” atom, based on which the
   * SQL schema is created. Whenever we need to import/query/...
   * an atom, we need to find its btom first.
   */
  protected object atom2btom extends (Atom[Term] => Btom[Var]) {
    val _index
    = schema.groupBy(btom => (btom.pred, btom.args.size))
      .mapValues(_ match {
        case List(singleValue) => singleValue
        case errList => throw new SchemaMismash(
          "Multiple Btoms have the same signature:" + errList )
      })

    def apply(a: Atom[Term]) : Btom[Var]
    = try { _index(( a.pred, a.args.size )) }
    catch {
      case cause: NoSuchElementException
        => throw new SchemaMismash("Atom was not found in the schema.", cause)
    }
  }

  /**
   * Every object in the FOL world is mapped to a name in the SQL world
   */
  protected object names {
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
  import names._

  /**
   * Opens the connection to an already-created database
   */
  def open = tryClose {new QueryExecutor}

  /**
   * Removes all tables from the database and recreates its structure
   */
  def reset() = { tryClose {
    schema.view // We need not to store the result
      .map (table) // Get SQL-escaped identifier
      .map { "DROP TABLE " + _ }
      .foreach { sql =>
        try { ada.execute(sql) } // DROPPING THE TABLE! BEWARE!!!
        catch { case _ => } // Ignore errors (hopefully they all are "table does not exist")
      }

    schema.view
      .map(btom => {
        "CREATE TABLE " + table(btom) +
          btom.variables
            .map(v => { col(btom)(v) + " " + ada.columnDefinition(v.dom)})
            .mkString(" ( ", " , ", " )")
      }).foreach{ada.execute}

    new DataImporter
  }}
  
  def close() { tryClose {ada.close} }

  class DataImporter extends IsEnabled {

    /**
     * Imports a single clause into the database
     *
     * This method is only callable between [[cernoch.sm.sql.SqlStorage.reset]]
     * and [[cernoch.sm.sql.SqlStorage.DataImporter.close]]. Calling this method
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
      ada.update(sql, cl.head.args) match {
        case 1 =>
        case _ => throw new BackendError("Clause was not added: " + cl)
      }
    }}

    def close() { tryClose(ada.close) }

    def done() = { tryClose {
      schema.foreach( btom => { btom.args.map(bvar => {
        "CREATE INDEX " + idx(btom)(bvar) + " ON " + table(btom) + " (" + col(btom)(bvar) + ")"
      }).foreach(ada.execute) })
      new QueryExecutor
    }}
  }

  /**
   * Executes SQL queries and returns
   */
  class QueryExecutor extends IsEnabled {

    def close() { tryClose(ada.close) }

    def query(q: Horn[Atom[Var], Set[Atom[FFT]]]) : Iterable[Map[Var, Val[_]]] = onlyIfEnabled {

      /**
       * Atoms are split into
       * a) standard atoms stored in an SQL-table (`tableAtoms`)
       * b) special atoms such as comparisons (`spešlAtoms`)
       */
      val (storeAtoms, spešlAtoms) =
        q.bodyAtoms.foldLeft(
          (List[Atom[FFT]](), List[Atom[FFT]]())
        )(
        (atomTuple,atom) => {
          val (tableA, spešlA) = atomTuple
          atom match {
            case a:LessThan[_] => (tableA, atom :: spešlA)
            case b:LessOrEq[_] => (tableA, atom :: spešlA)
            case _ => (atom :: tableA, spešlA)
          }
        }
      )

      /**
       * Assign a unique name to each atom in the query's body.
       *
       * Ideally, the atom's name equals to the name of its archetype.
       * But in cases like "parent(X,Y) /\ parent(Y,Z)", we must
       * distinguish between the two "parent" relation. To do so,
       * we simply use the [[cernoch.sm.sql.Tools.name]] function,
       * which automatically ensures uniqueness.
       */
      val atomTable = name(storeAtoms) { atom => table(atom2btom(atom)) }

      /**
       * Occurances of each variable in the schema
       */
      val avarsOcc = storeAtoms.view
        .map{_.variables}.flatten.toSet
        .map( (v:Var) => v -> {

          // Go over each body atom
          storeAtoms.toList.map(atom => {
           val btom = atom2btom(atom)

                // find variables in archetype
                (atom.args zip btom.args)
                  .filter { _._1 == v } // ...corresponding to "v" and
                  .map { _._2 } // (forgetting the original variable)
                  .map { (atom, btom, _) } // ...emit the table and the variable
              }).flatten
            }
      ).toMap

      /**
       * Helper create column name from
       */
      def cNam(v: (Atom[FFT], Btom[Var], Var))
        = atomTable(v._1) + "." + col(v._2)(v._3)

      /*
      * SELECT
      */
      // Name variables in the head
      val headCols = name (q.head.variables) {_.dom.name}


      // Create the select
      val SELECT = new ArrayBuffer[String]()
      avarsOcc
        .filterKeys(q.head.args.contains)
        .map { case (aVar, btomMapping) => {
          val (atom, btom, bVar) = btomMapping.head

          val aVarName = headCols(aVar)
          val bColName = col(btom)(bVar)

          SELECT += atomTable(atom) + "." + bColName +
            ( if (aVarName == bColName) "" else " AS " + aVarName )
        }}

      /*
       * FROM
       */
      val FROM = storeAtoms.map { atom => {
          if (atomTable(atom) == table(atom2btom(atom)))
            atomTable(atom) else table(atom2btom(atom)) + " AS " + atomTable(atom)
      }}

      /*
      * WHERE
      */
      val WHERE = new ArrayBuffer[String]()

      // Represent variable unification
      avarsOcc.values.foreach(toUnify => {
        neighbours(toUnify) foreach { case (col1, col2) => {
            WHERE += cNam(col1) + " = " + cNam(col2)
        }}
      })

      val BINDS = ArrayBuffer[Val[_]]()

      // Represent value binding
      storeAtoms.foreach(atom => {
        val btom = atom2btom(atom)

        // and values in the atom
        (atom.args zip btom.args).foreach {
          case (aVal:Val[_], bVar) => {
                BINDS += aVal
                WHERE += atomTable(atom) + "." + col(btom)(bVar) + " = ?"
          }
          case (_:Var, _) => // Safely ignore
        }
      })

      // Represent special atoms
      def varName(v:Var) = {
        val (_, btom, bvar) = avarsOcc(v).head
        col(btom)(bvar)
      }

      spešlAtoms.view.map(_ match {
        case lt:LessThan[_] => (" < ",  lt.x, lt.y)
        case le:LessOrEq[_] => (" <= ",  le.x, le.y)
      }).foreach { case (op, x, y) => {
        var result = ""

        def addTerm(t: Any) = t match {
          case v:Var => result = result + varName(v)
          case v:Val[_] => { result = result + "?"; BINDS += v }
        }

        addTerm(x)
        result = result + op
        addTerm(y)

        WHERE += result
      }}

      val sql =
        SELECT.mkString("SELECT ", ", ",    "") +
        mkStringIfNonEmpty( FROM )(" FROM ",  ", ",    "") +
        mkStringIfNonEmpty( WHERE)(" WHERE ", " AND ", "")

      /**  TODO: This code should be moved into [[cernoch.sm.sql.jdbc.JDBCAdaptor]] */
      val execute = () => ada.query(sql, BINDS.toList)

      /**  TODO: This code should be moved into [[cernoch.sm.sql.jdbc.JDBCAdaptor]] */
      val convert = (result: ResultSet) =>
        q.head.args.view.map{hVar =>
          hVar -> ada.extractArgument(result, headCols(hVar), hVar.dom)
        }.toMap

      /**  TODO: This code should be moved into [[cernoch.sm.sql.jdbc.JDBCAdaptor]] */
      new JDBCResConvert(execute)(convert)
    }
  }
}
