package cernoch.sm.sql.jdbc

import java.sql.Connection

/**
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
trait ConnectionCache
	extends JDBCAdaptor {

	protected var cache: Option[Connection] = None

	override def withConnection[T]
	(handler: Connection => T)
	= {
		val connection = cache.getOrElse {
			cache = Some(createCon)
			cache.get
		}
		try { handler(connection) }
		catch { case e: Throwable => {
			cache = None
			try { connection.close() }
			catch { case e: Throwable => {} }
			throw e
		}}
	}

	def close {
		cache.foreach(_.close())
		cache = None
	}
}
