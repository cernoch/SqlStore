package cernoch.sm.sql.jdbc

import java.sql.Connection

/**
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
trait ConnectionCache
	extends JDBCAdaptor {

	protected var cache: Option[Connection] = None

	override def withConnection[T](f: Connection => T)
	= {
		val connection = cache.getOrElse {
			cache = Some(createCon)
			cache.get
		}
		f(connection)
	}

	def close {
		cache.foreach(_.close())
		cache = None
	}
}
