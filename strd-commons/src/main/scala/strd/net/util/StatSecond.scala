package strd.net.util

import java.util.concurrent.atomic.AtomicLong

import strd.util.CTime

/**
 *
 * User: light
 * Date: 26/10/14
 * Time: 21:05
 */
@deprecated("Use TimerStats", "TimerStats")
case class StatSecond( flushTime : Long, count : AtomicLong = new AtomicLong() ) {
  def RPS = {
    val c = count.get()
    val now = CTime.now
    (c / ( (now - flushTime) / 1000d )).toInt
  }
}
