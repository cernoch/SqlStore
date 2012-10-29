package cernoch.sm.sql

class Throttler {
  var available = Runtime.getRuntime.availableProcessors()

  def acquire = synchronized {
    while (available <= 0) wait()
    available = available - 1
  }

  def release = synchronized {
    available = available + 1
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