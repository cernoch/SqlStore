package cernoch.sm.sql

import cernoch.scalogic.Atom

/**
 * Various tools.
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
object Tools {

	/**
	 * Pairs of neighbours in a list
	 */
	def neighbours[T](l: List[T])
	: List[(T, T)]
	= l match {
		case a :: b :: tail => (a, b) :: neighbours(b :: tail)
		case _ => Nil
	}

	/**
	 * Gives each item a name
	 *
	 * @param items Values which will receive a name
	 * @param index Gives each value a key, an "ideal name"
	 * @param clash Generates a unique key in case of a clash
	 */
	def name[T]
	(items: Iterable[T])
	(index: T => String = (x: T) => x.toString,
	 clash: ((String, Int) => String) = (_ + _))
	 =items.groupBy(index).map {
		case (key, vals) => vals.zipWithIndex.map {
			case (wal, index) => (wal, vals.size match {
				case 1 => key
				case _ => key + (1 + index)
			})
		}
	}.flatten.toMap

	/**
	 * RegEx for identifiers that need no escaping
	 */
	val SimpleIdent = "[a-zA-Z][a-zA-Z0-9]*".r

	/**
	 * Backslash-escapes a string if necessary
	 */
	def quote(s: String) = s match {
		case SimpleIdent() => s
		case _ => "\"" + s.replaceAll("\"", "\\\"") + "\""
	}

	/**
	 * (Grave-accent)-escapes a string if necessary
	 */
	def grave(s: String) = s match {
		case SimpleIdent() => s
		case _ => "`" + s.replaceAll("`", "``") + "`"
	}

	/**
	 * Signature of an atom.
	 *
	 * The signature maps atoms to tables in the SQL schema.
	 *
	 * @see [[cernoch.sm.sql.ArchetypeIndex]]
	 */
	def signature(b: Atom) = (b.pred, b.args.size)

	/**
	 * Crops a string to an arbitrary length
	 */
	def crop(s: String, max: Int = 5)
	= s.substring(0, math.min(s.length, max))
}
