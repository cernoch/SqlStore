package cernoch.scalogic.sql

import cernoch.scalogic._
import exceptions.SchemaMismash
import Tools._

/**
 * Every archetype is given a name in the SQL world.
 *
 * Ideally, the atom's name equals to the name of its archetype.
 * But in cases like "parent(X,Y) /\ parent(Y,Z)", we must
 * distinguish between the two "parent" relation. To do so,
 * we simply use the [[cernoch.scalogic.sql.Tools.name]] function,
 * which automatically ensures uniqueness.
 *
 * Additionally, this
 *
 * @param ada Adaptor providing correct escape routines
 * @param sch SQL schema as a list of atoms
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
private class Archetypes(ada: Adaptor, sch: List[Atom]) {

	/** Assign each atom in the sch a unique table name */
	val aom2sql = name(sch){_.pred}
	/** Assign each column in a table a name */
	val avr2sql = sch.map{ atom =>
		atom -> name(atom.args){_.dom.name}
	}.toMap

	/**  Escaped table name to be used directly in SQL */
	def aom2esc(atom: Atom) = ada.escapeTable(aom2sql(atom))
	/** Escaped column name to be used directly in SQL */
	def avr2esc(atom: Atom)(aVar: Var)
	= ada.escapeColumn(avr2sql(atom)(aVar))
	/** Escaped index name to be used directly in SQL */
	def idx2esc(atom: Atom)(aVar: Var)
	= ada escapeIndex(aom2sql(atom), avr2sql(atom)(aVar))



	/** Groups schema atoms by their signature */
	private val index = sch
		.groupBy(Tools.signature)
		.mapValues {
			case List(singleValue) => singleValue
			case errList => throw new SchemaMismash(
				s"Multiple modes have the same signature: $errList"
			)
		}

	/** Finds the respective atom in the schema */
	def apply(a: Atom)
	= try { index(Tools.signature(a)) }
	catch { case cause: NoSuchElementException => throw new
		SchemaMismash("Atom was not found in the schema.", cause)
	}
}
