package cernoch.scalogic.sql

/**
 * Keeps track of the object's state
 *
 * The object can be ``enabled`` or ``disabled``. When created,
 * the state is ``enabled``. The object can be disabled
 *
 * @author Radomír Černoch (radomir.cernoch at gmail.com)
 */
private[sql] trait IsEnabled {
  private var _enabled = true

  def enabled = _enabled

  /** If the object is enabled, the given method is called */
  protected def onlyIfEnabled[T](method: => T)
  = {
    if (!enabled) throw new IllegalStateException("Object has been closed.")
    method
  }

  /**
   * Calls the given method and sets ``enabled`` to false if no error occurs
   */
  protected def tryClose[T](method: => T)
  = {
    val out = onlyIfEnabled(method)
    _enabled = false
    out
  }
}
