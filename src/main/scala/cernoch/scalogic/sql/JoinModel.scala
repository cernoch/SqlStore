package cernoch.scalogic.sql

import Tools._

import cernoch.scalogic._
import cernoch.scalogic.tools.StringUtils._

import collection.mutable.{ListBuffer, ArrayBuffer}
import grizzled.slf4j.Logging
import java.util.NoSuchElementException

class JoinModel
	(ada: Adaptor,
	 som2aom: Archetypes,
	 body: Set[Atom])
	extends Logging {
	import som2aom._

	//debug(s"Executing query: ${q.toString(false,Labeler.alphabet)}")

	/**
	 * Atom is stored if it is not built-in
	 * */
	val sed = body.filter(atom => !BuiltInAtoms.canHandle(atom))

	/**
	 * Assign a unique name to each stored atom
	 *
	 * Ideally, the atom's name equals to the name of its archetype.
	 * But in cases like "parent(X,Y) /\ parent(Y,Z)", we must
	 * distinguish between the two "parent" relation. To do so,
	 * we simply use the [[cernoch.scalogic.sql.Tools.name]] function,
	 * which automatically ensures uniqueness.
	 */
	private val som2sql = name(sed){som =>
		Tools.crop(aom2sql(som2aom(som)))
	}

	/** Escaped unique name of a som */
	private def som2esc(som: Atom) = ada.escapeTable(som2sql(som))

	/**
	 * Maps each variable of a stored atom to its archetype var
	 */
	private val svr2avr = sed.flatMap(som =>
		(som.args zip som2aom(som).args)
			.collect{case (svr:Var,avr:Var) => (svr,avr)}
	).toMap

	/**
	 * Occurances of each variable in the schema
	 */
	private val svrOccs = (
		for (som <- sed; svr <- som.vars)
	yield (svr, (som,som2aom(som),svr2avr(svr)))
	).groupBy(_._1).mapValues{_.map{_._2}}



	/**
	 * Maps each queried variable into a column name
	 */
	private def occ2esc(occ: (Atom,Atom,Var)) = avr2esc(occ._2)(occ._3)
	private def occ2sql(occ: (Atom,Atom,Var)) = avr2sql(occ._2)(occ._3)

	/**
	 * Gives the column name for the given variable
	 */
	def varName(variable: Var)
	= try { Some(svr2esc(variable)) }
	catch { case _: NoSuchElementException => None }

	def queryBody = {
		val WHERE = WHERE_BINDS._1
		val BINDS = WHERE_BINDS._2

		val BODY =
			(  "FROM " |:: FROM  join ", " ) +
			(" WHERE " |:: WHERE join " AND ")

		BINDS.view.map{v => v.value match {
			case null => "NULL"
			case args => "'" + v.value.toString.replaceAll("'", "\\'") + "'"
		}}.foldLeft(BODY){_.replaceFirst("\\?", _)}
	}

	private def occ2escFull(occ: (Atom,Atom,Var)): String
	= (sed.size match {
		case 1 => ""
		case _ => som2esc(occ._1) + "."
	}) + occ2esc(occ)

	private def svr2esc(svr: Var): String = occ2escFull(svrOccs(svr).head)

	/*
	 * FROM
	 */
	private val FROM = sed.map{som => {
		var out = aom2esc(som2aom(som))
		if (som2sql(som) != aom2sql(som2aom(som)))
			out += " AS " + som2esc(som)
		out
	}}

	/*
	 * WHERE
	 */

	private val WHERE_BINDS = {

		val where = new ArrayBuffer[String]()
		val binds = new ListBuffer[Val]()

		// Represent variable unification
		svrOccs.values.foreach(toUnify => {
			neighbours(toUnify.toList)
				.foreach{ case (col1, col2) => {
					val sb = new StringBuilder()
					sb ++= occ2escFull(col1)
					sb ++= " = "
					sb ++= occ2escFull(col2)
					where += sb.toString()
				}}
			})

		// Represent value binding
		sed.foreach(som => {
			(som.args zip som2aom(som).args).foreach{
				case (svl:Val,avr:Var) => {
					where += som2esc(som) + "." + avr2esc(som2aom(som))(avr) + " = ?"
					binds += svl
				}
				case _ => // Safely ignore
			}
		})

		// Handle built-in atoms
		body.filter(BuiltInAtoms.canHandle).foreach(
			bom => where += BuiltInAtoms.toWHERE(binds,svr2esc)(bom))

		(where.toList, binds.toList)
	}

	/**
	 * SELECT command
	 */
	def select
		(head: Set[Var],
		 callback: Map[Var,Val] => Unit)
	= {
		val WHERE = WHERE_BINDS._1
		val BINDS = WHERE_BINDS._2

		// Name variables in the head
		val headCols = name(head){_.dom.name}

		// Create the select
		val SELECT = svrOccs.keySet.filter(head.contains).map(svr => {
			val sb = new StringBuilder()
			sb ++= svr2esc(svr)
			if (occ2sql(svrOccs(svr).head) != headCols(svr))
				sb ++= " AS " ++= headCols(svr)
			sb.toString()
		})

		val SQL =
			("SELECT " |:: SELECT mk  ", " ) +
			( " FROM " |::  FROM join ", " ) +
			(" WHERE " |:: WHERE join " AND ")

		debug(s"Translated into SQL query\n$SQL\n" +
			(Stream.from(1) zip BINDS) )

		val headDef = head.map{ hVar => (hVar, headCols(hVar), hVar.dom) }

		ada.withConnection(con => {
			ada.queryLimit match {

				// Adaptor does not support the LIMIT clause
				case None => {
					ada.query(con, SQL, BINDS, result => {
						while (result.next()) {

							val headMap = headDef.map {
								case (v,c,d) => v -> ada.extractArgument(result,c,d)
							}
							trace("Calling callback with values\n" + headMap)
							callback(headMap.toMap)
						}
					})
				}

				// Adaptor does support the LIMIT clause
				case Some(queryLimit) => {
					var skipped = 0
					var received = 0L
					do {
						received = 0
						val sqlLimited = s"$SQL LIMIT $skipped, $queryLimit"
						ada.query(con, sqlLimited, BINDS, result => {

							while (result.next()) {
								received += 1

								val headMap = headDef.map {
									case (v,c,d) => v -> ada.extractArgument(result,c,d)
								}

								trace("Calling callback with values\n" + headMap)
								callback(headMap.toMap)
							}
						})
						skipped += queryLimit
					} while (received >= queryLimit)
				}
			}
		})
	}

	/**
	 * How many items are there in the query?
	 */
	lazy val count = {
		val WHERE = WHERE_BINDS._1
		val BINDS = WHERE_BINDS._2

		trace("Initializing the 'count' value")

		val SQL = "SELECT COUNT(*)" +
			(" FROM "  |::  FROM join ", ") +
			(" WHERE " |:: WHERE join " AND ")

		debug(s"Translated into SQL query\n$SQL\n" +
			(BINDS zip Stream.from(1)).foreach(_.swap))

		ada.withConnection(con => {
			ada.query(con, SQL, BINDS, result => {
				result.next()
				result.getLong(1)
			})
		})
	}
}
