package cernoch.sm.sql

import cernoch.scalogic.{Var, Term, Atom}


private[sm] object Namer {

  def map2func
  [K, V]
  (m: Map[K, V])
  = (k: K) => m.get(k).get

  def temp[T](o: (String, Iterable[T])) = o match {
    case (key, vals) => vals.zipWithIndex.map {
      case (wal, index) => (wal, vals.size match {
        case 1 => key
        case _ => key + (1 + index)
      })
    }
  }


  /**
   * Gives each item a name
   *
   * @param items Values which will receive a name
   * @param index Gives each value a key, an "ideal name"
   * @param clash Generates a unique key in case of a clash
   */
  def name
  [T]
  (items: Iterable[T])
  (index: T => String = (x: T) => x.toString,
   clash: ((String, Int) => String) = (_ + _))
  =
    items.groupBy(index).map {
      case (key, vals) => vals.zipWithIndex.map {
        case (wal, index) => (wal, vals.size match {
          case 1 => key
          case _ => key + (1 + index)
        })
      }
    }.flatten.toMap

  /**
   * Assigns a unique name to each variable
   */
  def vars
  (vars: Iterable[Var])
  = name(vars) {
    _.dom.name.toUpperCase
  }
}