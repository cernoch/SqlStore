package cernoch.scalogic.sql

import cernoch.scalogic._
import collection.mutable.{ListBuffer, ArrayBuffer}
import tools.Labeler
import tools.StringUtils._
import Tools._
import grizzled.slf4j.Logging

/**
 * Executes SQL queries and returns variable bindings
 *
 * Terminology:
 *
 * atom ... Any atom
 * aom ... Archetype atoms (defines a SQL table)
 * som ... Stored atom in query's body (saved in a SQL table)
 * bom ... Built-in atom in query's body (not saved in a SQL table)
 *
 * avr ... Some variable inside aom
 * svr ... Some variable inside som
 * bvr ... Some variable inside bom
 *
 * ach ... Plural of aTom (mnemonic: ArCHetypes)
 * sed ... Plural of sTom (mnemonic: StorED)
 * bin ... Plural of bTom (mnemonic: Built-IN)
 *
 * sql ... Any identifier in the SQL schema
 * esc ... Escaped identified in the SQL schema
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
class SqlExecutor private[sql]
	(ada: Adaptor, sch: List[Atom])
	extends IsEnabled with Logging {

	/** Maps each som to its aom */
	private val som2aom = new ArchetypeIndex(sch)

	/** Maps each aom to its name */
	private val names = new ArchetypeNames(ada,sch); import names._

	def query
	(q: Horn[Set[Atom]],
	 callback: Map[Var,Val] => Unit )
	= onlyIfEnabled {

		debug(s"Executing query: ${q.toString(false,Labeler.alphabet)}")

		/**
		 * Atom is stored if it is not built-in
		 * */
		val sed = q.bodyAtoms.filter(atom => !BuiltInAtoms.canHandle(atom))

		/**
		 * Assign a unique name to each stored atom
		 *
		 * Ideally, the atom's name equals to the name of its archetype.
		 * But in cases like "parent(X,Y) /\ parent(Y,Z)", we must
		 * distinguish between the two "parent" relation. To do so,
		 * we simply use the [[cernoch.scalogic.sql.Tools.name]] function,
		 * which automatically ensures uniqueness.
		 */
		val som2sql = name(sed){som =>
			Tools.crop(aom2sql(som2aom(som)))
		}

		/** Escaped unique name of a som */
		def som2esc(som: Atom) = ada.escapeTable(som2sql(som))

		/**
		 * Maps each variable of a stored atom to its archetype var
		 */
		val svr2avr = sed.flatMap(som =>
			(som.args zip som2aom(som).args)
			.collect{case (svr:Var,avr:Var) => (svr,avr)}
		).toMap

		/**
		 * Occurances of each variable in the schema
		 */
		val svrOccs = (for (som <- sed; svr <- som.vars)
				yield (svr, (som,som2aom(som),svr2avr(svr)))
			).groupBy(_._1).mapValues{_.map{_._2}}



		/**
		 * Maps each queried variable into a column name
		 */

		def occ2esc(occ: (Atom,Atom,Var)) = avr2esc(occ._2)(occ._3)
		def occ2sql(occ: (Atom,Atom,Var)) = avr2sql(occ._2)(occ._3)

		def occ2escFull(occ: (Atom,Atom,Var))
		: String
		= (sed.size match {
			case 1 => ""
			case _ => som2esc(occ._1) + "."
		}) + occ2esc(occ)

		def svr2esc(svr: Var): String = occ2escFull(svrOccs(svr).head)

		/*
		 * SELECT
		 */
		// Name variables in the head
		val headCols = name(q.head.vars){_.dom.name}

		// Create the select
		val SELECT = new ArrayBuffer[String]()

		svrOccs.keySet
			.filter(q.head.args.contains)
			.foreach(svr => {
				val sb = new StringBuilder()
				sb ++= svr2esc(svr)
				if (occ2sql(svrOccs(svr).head) != headCols(svr))
					sb ++= " AS " ++= headCols(svr)
				SELECT += sb.toString()
			})

		/*
		 * FROM
		 */
		val FROM = sed.map{som => {
			var out = aom2esc(som2aom(som))

			if (som2sql(som) != aom2sql(som2aom(som)))
				out += " AS " + som2esc(som)

			out
		}}

		/*
		 * WHERE
		 */
		val WHERE = new ArrayBuffer[String]()
		val BINDS = new ListBuffer[Val]()

		// Represent variable unification
		svrOccs.values.foreach(toUnify => {
			neighbours(toUnify.toList)
				.foreach{ case (col1, col2) => {
					val sb = new StringBuilder()
					sb ++= occ2escFull(col1)
					sb ++= " = "
					sb ++= occ2escFull(col2)
					WHERE += sb.toString()
				}}
			})

		// Represent value binding
		sed.foreach(som => {
			(som.args zip som2aom(som).args).foreach{
				case (svl:Val,avr:Var) => {
					WHERE += som2esc(som) + "." + avr2esc(som2aom(som))(avr) + " = ?"
					BINDS += svl
				}
				case _ => // Safely ignore
			}
		})

		// Handle built-in atoms
		q.bodyAtoms.filter(BuiltInAtoms.canHandle).foreach(
			bom => WHERE += BuiltInAtoms.toWHERE(BINDS,svr2esc)(bom))

		val SQL =
			("SELECT "|:: SELECT mk   ", " ) +
			( " FROM "|::  FROM  join ", " ) +
			(" WHERE "|:: WHERE  join " AND ")

		debug(s"Translated into SQL query\n$SQL\n" +
			(BINDS zip Stream.from(1)).foreach(_.swap))

		ada.withConnection(con => {
			ada.query(con, SQL, BINDS.toList, result => {
				while (result.next()) {

					val headMap =
						for (hVar <- q.head.vars)
							yield {
								val col = headCols(hVar)
								val dom = hVar.dom
								hVar -> ada.extractArgument(result, col, dom)
							}

					trace("Calling callback with values\n" + headMap)
					callback(headMap.toMap)
				}
			})
		})
	}
}
