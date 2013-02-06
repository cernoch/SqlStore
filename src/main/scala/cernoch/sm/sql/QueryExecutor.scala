package cernoch.sm.sql

import jdbc.{JDBCAdaptor, JDBCResConvert}
import cernoch.scalogic._
import numeric.{LessOrEq, LessThan}
import cernoch.sm.sql.Tools._
import cernoch.scalogic._
import collection.mutable.ArrayBuffer
import tools.StringUtils._
import java.sql.ResultSet

/**
 * Executes SQL queries and returns variable bindings
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class QueryExecutor private[sql]
    (ada: JDBCAdaptor, schema: List[Btom[Var]])
    extends IsEnabled {

  private val atom2btom = new Atom2Btom(schema)
  private val names = new SchemaNames(ada, schema)
  import names._

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
