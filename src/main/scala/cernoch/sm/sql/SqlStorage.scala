package cernoch.sm.sql

import cernoch.scalogic._
import cernoch.scalogic.storage._
import exceptions.SchemaMismash
import java.sql.{ResultSet}
import collection.mutable.{ArrayBuffer}


class SqlStorage(
    ada: JDBCAdaptor,
    schema: List[BLC[Btom[Var]]])
  extends Transactioned[
    Queriable[Horn[Atom[FFT], Set[Atom[FFT]]], Val[_]],
    BLC[Atom[Val[_]]]] {
  storage =>

  /**
   * Assign each atom in the schema a unique table name
   */
  private val tableNamer
  = Namer.name(schema.map{_.head}){_.head.pred}


  /**
   * Assign each column in a table a name
   */
  private val columnNamer
  = schema.map{ btom =>
    btom.head -> Namer.name(btom.head.args){_.dom.name}
  }.toMap

  /**
   * Escaped table name to be used directly in SQL
   */
  protected def nameTable
    (btom: Btom[Var])
  = ada.escTab(tableNamer(btom))

  /**
   * Escaped column name to be used directly in SQL
   */
  protected def nameColumn
    (btom: Btom[Var])
    (bvar: Var)
  = ada.escapeColumn(
    columnNamer(btom)(bvar)
  )

  /**
   * Escaped index name to be used directly in SQL
   */
  protected def nameIndex
    (btom: Btom[Var])
    (bvar: Var)
  = ada.escapeIndex(
    tableNamer(btom) + "_" +
    columnNamer(btom)(bvar)
  )



  /**
   * Open the ScalaMiner connection to a already-created table
   */
  def open = new Connection

  /**
   * Removes all tables from the database and recreates its structure
   */
  def reset = {
    schema.map { _.head } // For each table name
      .map(nameTable) // Get SQL-escaped identifier
      .map { "DROP TABLE " + _ }
      .foreach { s =>
        try {
          ada.execute(s)
        } // DROPPING THE TABLE! BEWARE!!!
        catch {
          case _ =>
        } // Ignore errors (usually they are "table does not exist")
      }

    schema.map { _.head }.map(btom => {
      "CREATE TABLE " +
        nameTable(btom) +
        btom.variables.map(v => {
          nameColumn(btom)(v) + " " + ada.columnDefinition(v.dom)
        }).mkString(" ( ", " , ", " )")
      })
      .foreach{ada.execute}

    new SqlImporter
  }


  class SqlImporter extends Importer {

    var enabled = true

    val throttle = new Throttler

    /**
     * Imports a single clause into the database
     *
     * This method is only callable between [[cernoch.sm.sql.SqlStorage.reset]]
     * and [[cernoch.sm.sql.SqlStorage.Importer.close]]. Calling this method
     * inbetween will throw an error.
     *
     * Since the SQL databases work with relational calculus, no variables,
     * nor function symbols are allowed. Hence only {{{Atom[Val[_]]}}}
     * is allowed.
     *
     * As SQL is not a deductive database, clauses mustn't have bodies.
     */
    def put(cl: BLC[Atom[Val[_]]]) {
      if (!enabled) throw new Exception(
        "Importer not initialized! Check logs for errors.")

      // Find all atoms in the schema that match the imported clause.
      val btom = schema find(btom => {
                 btom.head.pred == cl.head.pred &&
            btom.head.args.size == cl.head.args.size
        }) getOrElse {
          throw new SchemaMismash(
            "Atom not found in the schema: " + cl.head)
        } head

      // Create a dummy query
      val sql = "INSERT INTO " +
        nameTable(btom) +
        btom.args.map{ _ => "?" }
          .mkString(" VALUES ( ", ", ", " )")

      // And execute!
      ada.update(sql, cl.head.args) match {
        case 1 =>
        case _ => throw new Exception("Cannot add the clause: " + cl)
      }
    }

    def close
    : Queriable[Horn[Atom[FFT],Set[Atom[FFT]]], Val[_]]
    = {
      enabled = false

      schema
        .map{ _.head }
        .map(btom => {
          btom.args.map(v => {
            "CREATE INDEX " +
              ada.escapeIndex(nameTable(btom) + "_" +
                         nameColumn(btom)(v)) + " ON " +
              nameTable(btom) + " (" +
              nameColumn(btom)(v) + ")"
          })
          .map(ada.execute)
        })

      new Connection
    }
  }

  class Connection
    extends Queriable[Horn[Atom[FFT], Set[Atom[FFT]]], Val[_]] {

    private def neighbours
    [T]
    (l: List[T])
    : List[(T, T)]
    = l match {
      case a :: b :: tail => (a, b) :: neighbours(b :: tail)
      case _ => Nil
    }

    def close = ada.close

    def query
      (q: Horn[Atom[FFT], Set[Atom[FFT]]])
    : Iterable[Map[Var, Val[_]]]
    = {

      /**
       * Assign a unique name to each atom in the query's body.
       *
       * Ideally, the atom's name equals to the name of its archetype.
       * But in cases like "parent(X,Y) /\ parent(Y,Z)", we must
       * distinguish between the two "parent" relation. To do so,
       * we simply use the '''Namer.name''' function, which
       * automatically ensures uniqueness.
       */
      val atomName = Namer.name(q.bodyAtoms) {
        atom =>
          nameTable(schema.find {
            _.head.pred == atom.pred
          }.get.head)
      }

      /**
       * Maps each atom (query) to the btom (schema)
       */
      val atomBtom = q.bodyAtoms.map {
        atom =>
          atom -> schema.find {
            _.head.pred == atom.pred
          }.get.head
      }.toMap

      /**
       * Occurances of each variable in the schema
       */
      val avarsOcc = q.variables.toSet.map((v: Var) =>
        v -> {

          // Go over each body atom
          q.bodyAtoms.toList.map(atom => {
            val btom = atomBtom(atom)

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
      = atomName(v._1) + "." + nameColumn(v._2)(v._3)

      /*
       * SELECT
       */

      // Name variables in the head
      val headCols = Namer.vars(q.head.variables)

      // Create the select
      val SELECT = new ArrayBuffer[String]()
      avarsOcc
        .filterKeys(q.head.args.contains)
        .map { case (aVar, occs) => {

          val (atom, btom, bVar) = occs.head
        
          val nameForA = atomName(atom)
          val bColName = nameColumn(btom)(bVar)
        
          val aVarName = headCols(aVar)

          SELECT +=
            nameForA + "." + bColName +
              ( if (aVarName == bColName)
                "" else " AS " + aVarName )
        }}

      /*
       * FROM
       */
      val FROM = q.bodyAtoms.map {
        atom => {
          val relation = nameTable(atomBtom(atom))
          val nameForA = atomName(atom)
          
          if (relation == nameForA)
            relation else relation + " AS " + nameForA
        }
      }

      /*
       * WHERE
       */
      val WHERE = new ArrayBuffer[String]()

      // Represent variable unification
      avarsOcc.values.foreach(toUnify => {

        neighbours(toUnify) foreach {
          case (col1, col2) => {
            WHERE += cNam(col1) + " = " + cNam(col2)
          }
        }
      })

      val BINDS = ArrayBuffer[Val[_]]()

      // Represent value binding
      q.bodyAtoms.foreach(atom => {
        val btom = atomBtom(atom)

        // and values in the atom
        (atom.args zip btom.args).foreach {
          case (aFFT, bVar) =>
            aFFT match {
              case aVal: Val[_] => {
                BINDS += aVal
                WHERE +=
                  atomName(atom) + "." +
                  nameColumn(btom)(bVar) + " = ?"
              }
              case _ =>
            }
        }
      })

      val sql =
        SELECT.mkString("SELECT ", ", ",    "") +
          FROM.mkString(" FROM ",  ", ",    "") +
         WHERE.mkString(" WHERE ", " AND ", "")

      // Result iterable
      new ResultIterable[Map[Var, Val[_]]](sql, BINDS.toList, result => {

        q.head.args.foldLeft(
          Map[Var,Val[_]]()
        ){ (map,term) => term match {

          // Map each variable
          case hVar:Var =>
            map + (hVar -> ada.extractArgument(
              result, headCols(hVar), hVar.dom ))

          // Ignore other values
          case _ => map
        }}
      })
    }
  }

  /**
   * Helper class which iterates over results from a SELECT query
   *
   * Typically there is a query `q(X) <- a(X,Y), b(Y)`, whose body
   * translates into SQL and gets executed. This class helps to
   * convert the result of the query into the original type of `q(X)`.
   *
   * @param getNext Creates a new result from the query output
   * @tparam T Type of the result
   */
  class ResultIterable[T]
    (sql: String,
     values: List[Val[_]],
     getNext: ResultSet => T)
    extends Iterable[T] {

    def iterator = new Iterator[T] {

      /**
       * Execute the query every time we iterate over the results
       */
      val result = ada.query(sql, values)

      /**
       * Availability of the next result
       */
      var hasnext = result.next()

      def hasNext = hasnext

      def next = {
        val out = getNext(result)
        hasnext = result.next()
        out
      }
    }
  }
}
