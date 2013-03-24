package cernoch.scalogic.sql

import cernoch.scalogic._
import tools.Labeler
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
	(ada: Adaptor, som2aom: Archetypes)
	extends IsEnabled
	with Logging {

	def prepare(body: Set[Atom])
	= new JoinModel(ada, som2aom, body)

	@Deprecated
	def query
	(query: Horn[Set[Atom]],
	 callback: Map[Var,Val] => Unit )
	{ onlyIfEnabled {

		debug(s"Executing query: " +
			query.toString(short=false,names=Labeler.alphabet))

		new JoinModel(ada, som2aom, query.bodyAtoms)
			.select(query.head.vars.toSet, callback)
	}}
}
