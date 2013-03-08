package cernoch.sm.sql

import jdbc.JDBCAdaptor
import cernoch.sm.sql.Tools._
import cernoch.scalogic._
import collection.mutable.ArrayBuffer
import tools.StringUtils._
import scala.Predef._

/**
 * Executes SQL queries and returns variable bindings
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class QueryExecutor private[sql]
    (ada: JDBCAdaptor, schema: List[Mode])
    extends IsEnabled {

  private val atom2btom = new Atom2Btom(schema)
  private val names = new SchemaNames(ada, schema)
  import names._

  def query
  ( q: Horn[Set[Atom]],
    callback: Map[Var,Val] => Unit )
  = onlyIfEnabled {

		/**
     * Atoms are split into
     * a) standard atoms stored in an SQL-table (`tableAtoms`)
     * b) special atoms such as comparisons (`spešlAtoms`)
     */
    val (storeAtoms, spešlAtoms) =
      q.bodyAtoms.foldLeft(
        (List[Atom](), List[Atom]())
      )(
        (atomTuple,atom) => {
          val (tableA, spešlA) = atomTuple
          atom.pred match {
						case "<"  => (tableA, atom :: spešlA)
						case ">"  => (tableA, atom :: spešlA)
						case "<=" => (tableA, atom :: spešlA)
						case ">=" => (tableA, atom :: spešlA)
						case "<>" => (tableA, atom :: spešlA)
						case "!=" => (tableA, atom :: spešlA)
						case "IN" => (tableA, atom :: spešlA)
						case "!IN"=> (tableA, atom :: spešlA)
            case  _   => (atom :: tableA, spešlA)
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
    def crop(s: String) = s.substring(0, math.min(s.length, 5))
    val _atomTable = name(storeAtoms) { atom => crop(_tables(atom2btom(atom))) }
    val atomTable = (a:Atom) => ada.escapeTable(_atomTable(a))

		def varValPairs(m:Mode,a:Atom)
		= (m.args zip a.args).collect{case (a:Var,b) => (a,b)}

    /**
     * Occurances of each variable in the schema
     */
    import Atom2Btom._
    val avarsOccRaw = ( for (
        atom <- storeAtoms;
        aVar <- atom.vars;
        btom = atom2btom(atom)
        if sign(atom) == sign(btom);
        (bVar, aArg) <- varValPairs(btom,atom)
        if aVar == aArg) yield (aVar, (atom, btom, bVar))
    )
    val avarsOcc = avarsOccRaw
      .groupBy(_ _1)
      .mapValues{ _ map { _._2 } }

    /**
     * Helper create column name from
     */
    def cNam(v: (Atom, Mode, Var))
    = atomTable(v._1) + "." + col(v._2)(v._3)

    /*
    * SELECT
    */
    // Name variables in the head
    val headCols = name (q.head.vars) {_.dom.name}

    // Represent special atoms
    def varName(v:Var) = {
      val (_, btom, bvar) = avarsOcc(v).head
      col(btom)(bvar)
    }

    // Create the select
    val SELECT = new ArrayBuffer[String]()
    avarsOccRaw.map{_._1}.distinct
      .filter(q.head.args.contains).foreach(aVar => {
        val (atom,_,_) = avarsOcc(aVar).head
        val aVarName = headCols(aVar)
        val bColName = varName(aVar)

        SELECT +=
          (if (storeAtoms.size == 1) "" else atomTable(atom) + ".") +
          bColName +
          ( if (aVarName == bColName) "" else " AS " + aVarName )
    })

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

    val BINDS = ArrayBuffer[Val]()

    // Represent value binding
    storeAtoms.foreach(atom => {
      val btom = atom2btom(atom)

      // and values in the atom
			varValPairs(btom,atom).foreach {
        case (bVar, aVal:Val) => {
          BINDS += aVal
          WHERE += atomTable(atom) + "." + col(btom)(bVar) + " = ?"
        }
        case _ => // Safely ignore
      }
    })

		def addTerm(t: Term, result: StringBuilder) = t match {
			case aVal:Val => { result ++= "?"; BINDS += aVal }
			case aVar:Var => {
				val aName = storeAtoms.size match {
					case 1 => ""
					case _ => atomTable(avarsOcc(aVar).head._1) + "."
				}
				result ++= aName ++= varName(aVar)
			}
			case _ => throw new IllegalArgumentException(
				"Can only handle values and variables" )
		}

		val binary = List("<","<=",">",">=","<>","!=","==")
		spešlAtoms.view
			.filter(atom => binary.contains(atom.pred))
			.map( atom => (atom.pred, atom.args(0), atom.args(1)) )
			.foreach { case (op, x, y) => {
				var result = new StringBuilder()
				addTerm(x, result)
				result ++= " " ++= op ++= " "
				addTerm(y, result)
				WHERE += result.toString()
			}}

		spešlAtoms.view
			.filter(_.pred == "IN")
			.foreach(atom => {
			var result = new StringBuilder()
			addTerm(atom.args.head, result)
			result ++= " IN ("
			atom.args.tail.foreach(t => addTerm(t, result))
			result ++= ")"
			WHERE += result.toString()
		})

		spešlAtoms.view
			.filter(_.pred == "!IN")
			.foreach(atom => {
			var result = new StringBuilder()
			result ++= "NOT("
			addTerm(atom.args.head, result)
			result ++= " IN ("
			atom.args.tail.foreach(t => addTerm(t, result))
			result ++= "))"
			WHERE += result.toString()
		})

		val sql =
			("SELECT "|:: SELECT mk   ", " ) +
			( " FROM "|::  FROM  join ", " ) +
			(" WHERE "|:: WHERE  join " AND ")

		ada.withConnection(con => {
			ada.query(con, sql, BINDS.toList)(result => {
				while (result.next()) callback(
					q.head.vars.view.map(hVar => {
						hVar -> ada.extractArgument(
							result, headCols(hVar), hVar.dom)
						}).toMap
					)
  		})
		})
  }
}
