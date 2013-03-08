package cernoch.sm.sql.jdbc

import java.sql.Connection

trait ResettingCache
	extends ConnectionCache {

	/**
	 * Number of [[cernoch.sm.sql.jdbc.JDBCAdaptor.withConnection]]
	 * calls with the current connection
	 */
	protected var usageCounter = 0

	protected var usageLimit = 1000

	override def withConnection[T](f: Connection => T)
	= {
		usageCounter = usageCounter + 1
		if (usageCounter > usageLimit) {
			usageCounter = 0
			close
		}
		super.withConnection(f)
	}
}
