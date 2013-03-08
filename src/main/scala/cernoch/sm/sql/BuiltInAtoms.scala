package cernoch.sm.sql

import cernoch.scalogic._
import collection.generic.Growable

/**
 * Handling atoms built into the SQL language (not stored on disk)
 */
object BuiltInAtoms {
	implicit def item2self[T](a:T) = a->a

	/** Atoms with 2 arguments */
	object Binary {
		val dictionary = Map(
			"<","<=",">",">=",
			"<>"->"!=", "!=" )

		def canHandle(a:Atom)
		= a.args.length == 2 && dictionary.contains(a.pred)

		def unapply(a:Atom)
		= if (!canHandle(a)) None else
			Some(dictionary(a.pred), a.args(0), a.args(1))
	}

	/** Atoms with at least 2 arguments */
	object MinBinary {
		val dictionary = Map("IN", "member"->"IN",
			"NOT IN", "!IN"->"NOT IN", "\\+ member"->"NOT IN")

		def canHandle(a:Atom)
		= a.args.length >= 2 && dictionary.contains(a.pred)

		def unapply(a:Atom)
		= if (!canHandle(a)) None else
			Some(dictionary(a.pred), a.args)
	}

	def canHandle(a:Atom)
	= Binary.canHandle(a) ||
		MinBinary.canHandle(a)

	/**
	 * Appends a built-in atom into the WHERE clause
	 *
	 * @param BINDS Value bindings
	 * @param taVar Name of the table+column for a variable
	 * @param a The built-in atom to add
	 * @return
	 */
	def toWHERE
	(BINDS: Growable[Val],
	 taVar: Var => String)
	(a:Atom) = a match {

		case Binary(op,x,y) => {
			val s = new StringBuilder()
			s ++= term(BINDS,taVar)(x)
			s ++= " " ++= op ++= " "
			s ++= term(BINDS,taVar)(y)
			s.toString()
		}

		case MinBinary("IN", v :: value :: other) => {
			val s = new StringBuilder()
			s ++= term(BINDS,taVar)(v)
			s ++= " IN ("
			(value :: other).foreach( t =>
				s ++= term(BINDS,taVar)(t) )
			s ++= ")"
			s.toString()
		}

		case MinBinary("NOT IN", v :: value :: other) => {
			val s = new StringBuilder()
			s ++= "NOT("
			s ++= term(BINDS,taVar)(v)
			s ++= " IN ("
			(value :: other).foreach( t =>
				s ++= term(BINDS,taVar)(t) )
			s ++= "))"
			s.toString()
		}
	}

	private def term
	(BINDS: Growable[Val],
	 taVar: Var => String)
	(t: Term) = t match {
		case v:Val => { BINDS += v; "?" }
		case v:Var => { taVar(v) }
		case _:Fun => throw new
				IllegalArgumentException(
					"Functions are not supported")
	}
}

