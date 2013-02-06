package cernoch.sm.sql

import cernoch.scalogic.{Var, Btom, Term, Atom}
import exceptions.SchemaMismash

/**
 * Fast index for mapping arbitrary atoms to their representative btoms.
 *
 * Note: Btom in this context is a “prototype” atom, based on which the
 * SQL schema is created. Whenever we need to import/query/...
 * an atom, we need to find its btom first.
 */
private class Atom2Btom(schema: List[Btom[Var]])
    extends (Atom[Term] => Btom[Var]) {

  val _index = schema
    .groupBy(btom => (btom.pred, btom.args.size))
    .mapValues(_ match {
    case List(singleValue) => singleValue
    case errList => throw new SchemaMismash(
      "Multiple Btoms have the same signature:" + errList )
  })

  def apply(a: Atom[Term]) : Btom[Var]
  = try { _index(( a.pred, a.args.size )) }
  catch {
    case cause: NoSuchElementException
    => throw new SchemaMismash("Atom was not found in the schema.", cause)
  }
}
