package cernoch.sm.sql

import cernoch.scalogic._
import cernoch.scalogic.storage._
import exceptions.SchemaMismash
import java.sql.{ResultSet, PreparedStatement}
import collection.mutable.{HashMap, ArrayBuffer}


class SqlStorage(
    sett: SqlSettings,
    schema: List[BLC[Btom[Var]]])
  extends Transactioned[
    Queriable[Horn[Atom[FFT], Set[Atom[FFT]]], Val[_]],
    BLC[Atom[Val[_]]]] {
  storage =>

  // Create a JDBC connection to the database
  private val con = sett()

  // Assign each atom in the schema a table name
  private val tables = Namer.atoms(sett.prefix, schema.map {
    _.head
  })

  // Assign each column in a table a name
  private val cols = Namer.map2func(schema.map {
    btom => btom.head -> Namer.vars(btom.head.args)
  }.toMap)

  private def i(str: String)
  = if (str matches "[a-zA-Z]+")
      str else "\"" + str + "\""

  /**
   * Open the ScalaMiner connection to a already-created table
   */
  def open = new Connection

  /**
   * Removes all tables from the database and recreates its structure
   */
  def reset = {
    schema.map { _.head } // For each table name
      .map(tables).map { i } // Get SQL-escaped identifier
      .map { "DROP TABLE " + _ }
      .foreach { s =>
        try {
          con.prepareStatement(s).execute
        } // DROPPING THE TABLE! BEWARE!!!
        catch {
          case _ =>
        } // Ignore errors (usually they are "table does not exist")
      }

    schema.map { _.head }.map(btom => {
      "CREATE TABLE " + i(tables(btom)) +
        btom.variables.map(v => {
          i(cols(btom)(v)) + " " + sett.domainName(v.dom)
        }).mkString(" ( ", " , ", " )")
      })
      .map {sql =>
        try {
          val st = con.prepareStatement(sql)
          st.execute
        } catch {
          case x =>
            println("Cannot create table using >>>" + sql + "<<<:\n" + x)
            throw x
        }
      }

    new SqlImporter
  }


  class SqlImporter extends Importer {

    if (sett.transactions) {
      con.prepareCall("BEGIN TRANSACTION").execute()
    }

    var enabled = true
    //if (!enabled) throw new Exception("Cannot start a session!")

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
      val btom = schema.map {
        _.head
      }
        .find(btom => {
        btom.pred == cl.head.pred &&
          btom.args.size == cl.head.args.size
      })
        .getOrElse {
        throw new SchemaMismash(
          "Atom not found in the schema: " + cl.head)
      }

      // Create a dummy query
      val sql = con.prepareStatement(
        "INSERT INTO " + i(tables(btom)) +
          btom.args.map {
            _ => "?"
          }.mkString(" VALUES ( ", ", ", " )")
      )

      // Set the query's parameters according to the imported atom
      cl.head.args.zip(Stream.from(1)).foreach {
        case (arg, i) => {
          arg match {
            case Cat(null) => sql.setNull(i, java.sql.Types.VARCHAR)
            case Num(null) => sql.setNull(i, java.sql.Types.INTEGER)
            case Dec(null) => sql.setNull(i, java.sql.Types.DOUBLE)
            case Cat(v) => sql.setString(i, v)
            case Num(v) => sql.setInt(i, v.toInt)
            case Dec(v) => sql.setBigDecimal(i, v.bigDecimal)
            case x => throw new Exception("Unknown value: " + x)
          }
        }
      }

      // And execute!
      sql.executeUpdate() match {
        case 1 =>
        case _ => throw new Exception("Cannot add the clause: " + cl)
      }
    }

    def close: Connection = {
      enabled = false

      if (sett.transactions) {
        con.prepareCall("COMMIT").execute()
      }

      schema
        .map {
        _.head
      }
        .map(btom => {
        btom.args.map(v => {
          "CREATE INDEX " +
            i(tables(btom) + "-" + cols(btom)(v)) + " ON " +
            i(tables(btom)) + " (" +
            i(cols(btom)(v)) + ")"
        })
          .map {
          con.prepareStatement
        }
          .foreach {
          _.execute
        }
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


    def query
      (q: Horn[Atom[FFT], Set[Atom[FFT]]])
    = {

      //println(q.head + " :- " + q.body.mkString(", ") + ".")
      
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
          tables(schema.find {
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
      = i(atomName(v._1)) + "." +
        i(cols(v._2)(v._3))

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
          val bColName = cols(btom)(bVar)
        
          val aVarName = headCols(aVar)

          SELECT +=
            i(nameForA) + "." + i(bColName) +
              ( if (aVarName == bColName)
                "" else " AS " + i(aVarName) )
        }}

      /*
       * FROM
       */
      val FROM = q.bodyAtoms.map {
        atom => {
          val relation = tables(atomBtom(atom))
          val nameForA = atomName(atom)
          
          if (relation == nameForA)
            i(relation) else i(relation) + " AS " + i(nameForA)
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
                  i(atomName(atom)) + "." +
                    i(cols(btom)(bVar)) + " = ?"
              }
              case _ =>
            }
        }
      })

      val sqlStr =
        SELECT.mkString("SELECT ", ", ",    "") +
          FROM.mkString(" FROM ",  ", ",    "") +
         WHERE.mkString(" WHERE ", " AND ", "")

      val sql = con.prepareCall(sqlStr)

      (BINDS zip Stream.from(1)) foreach {
        case (v, i) =>
          v match {
            case Cat(null) => sql.setNull(i, java.sql.Types.VARCHAR)
            case Num(null) => sql.setNull(i, java.sql.Types.INTEGER)
            case Dec(null) => sql.setNull(i, java.sql.Types.DOUBLE)
            case Cat(v)    => sql.setString(i, v)
            case Num(v)    => sql.setInt(i, v.toInt)
            case Dec(v)    => sql.setBigDecimal(i, v.bigDecimal)
            case x => throw new Exception("Unknown value: " + x)
          }
      }

      // Result iterable
      new ResultIterable[Map[Var, Val[_]]](sql, result => {

        q.head.args.foldLeft(
          Map[Var,Val[_]]()
        ){ (map,term) => term match {

          // Map each variable
          case hVar:Var => {
            // Name of the headVar's column
            val col = headCols(hVar)
            // Get the result by the headVar name
            map + (hVar -> (hVar.dom match {
              case d@DecDom(_) => new Dec(BigDecimal(result.getBigDecimal(col)), d)
              case d@NumDom(_, _) => new Num(BigInt(result.getInt(col)), d)
              case d@CatDom(_, _, _) => new Cat(result.getString(col), d)
            }).asInstanceOf[Val[_]])
          }

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
   * @param statement Statement to execute
   * @param getNext Creates a new result from the query output
   * @tparam T Type of the result
   */
  class ResultIterable[T]
    (statement: PreparedStatement,
     getNext: ResultSet => T)
    extends Iterable[T] {

    def iterator = new Iterator[T] {
      /**
       * Execute the query every time we iterate over the results
       */
      val result = statement.executeQuery()

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
