package cernoch.sm.sql.jdbc

import java.sql.ResultSet

/**
 * Helper class which iterates over results from a SELECT query
 *
 * Typically there is a query `q(X) <- a(X,Y), b(Y)`, whose body
 * translates into SQL and gets executed. This class helps to
 * convert the result of the query into the original type of `q(X)`.
 *
 * @param execute Executes the SQL query
 * @param convert Creates a new result from the query output
 * @tparam T Type of the result
 */
class JDBCResConvert[T]
  (execute: () => ResultSet)
  (convert: ResultSet => T)
  extends Iterable[T] {

  def iterator = new Iterator[T] {

    /**Execute the query every time we iterate over the results */
    val result = execute()

    /**Availability of the next result */
    var hasnext = result.next()

    def hasNext = hasnext

    def next = {
      val out = convert(result)
      hasnext = result.next()
      out
    }
  }
}
