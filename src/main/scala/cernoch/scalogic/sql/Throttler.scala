package cernoch.scalogic.sql

/**
 * Method [[cernoch.scalogic.sql.Throttler#exec()]] can be executed at
 * most [[cernoch.scalogic.sql.Throttler#limit]] times simultaneously.
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