package cernoch.sm.sql

/**
 * Method [[cernoch.sm.sql.Throttler.exec()]] can be executed at
 * most [[cernoch.sm.sql.Throttler.limit]] times simultanously.
 */
class Throttler {
  var limit = Runtime.getRuntime.availableProcessors()

  def acquire = synchronized {
    while (limit <= 0) wait()
    limit = limit - 1
  }

  def release = synchronized {
    limit = limit + 1
    notify()
  }

  def exec(body: => Unit) {
    try {
      acquire
      body
    } finally {
      release
    }
  }
}