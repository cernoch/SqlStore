package cernoch.sm.sql

import cernoch.scalogic._
import exceptions.SchemaMismash

/**
 * Fast index maps atom to its archetype.
 *
 * @param ach List of all archetypes
 */
private class ArchetypeIndex(ach: List[Atom])
		extends (Atom => Atom) {

	private val index = ach
		.groupBy(Tools.signature)
		.mapValues{
			case List(singleValue) => singleValue
			case errList => throw new SchemaMismash(
				"Multiple modes have the same signature: " + errList )
		}

	def apply(a: Atom)
	= try { index(Tools.signature(a)) }
	catch { case cause: NoSuchElementException => throw new
		SchemaMismash("Atom was not found in the schema.", cause)
	}
}
