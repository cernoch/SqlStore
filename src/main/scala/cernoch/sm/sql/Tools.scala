package cernoch.sm.sql

/**
 * Various tools for the ScaLogic => SQL package
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
private object Tools {

  def neighbours
  [T]
  (l: List[T])
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
}
