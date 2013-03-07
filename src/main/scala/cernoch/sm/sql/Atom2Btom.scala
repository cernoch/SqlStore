package cernoch.sm.sql

import cernoch.scalogic._
import exceptions.SchemaMismash

/**
 * Fast index for mapping arbitrary atoms to their representative btoms.
 *
 * Note: Btom in this context is a “prototype” atom, based on which the
 * SQL schema is created. Whenever we need to import/query/...
 * an atom, we need to find its btom first.
 */
private class Atom2Btom(schema: List[Mode])
		extends (Atom => Mode) {

	private lazy val index = schema
		.groupBy(Atom2Btom.sign)
		.mapValues(_ match {
			case List(singleValue) => singleValue
			case errList => throw new SchemaMismash(
				"Multiple Btoms have the same sign:" + errList )
		})

	def apply(a: Atom) : Mode
	= try {
		index(( a.pred, a.args.size ))
	} catch {
	case cause: NoSuchElementException => throw new
		SchemaMismash("Atom was not found in the schema.", cause)
	}
}

private object Atom2Btom {
  def sign(b: Atom) = (b.pred, b.args.size)
	def sign(b: Mode) = (b.pred, b.args.size)
}
