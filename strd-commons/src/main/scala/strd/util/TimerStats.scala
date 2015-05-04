package strd.util

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
 * @author Kirill chEbba Chebunin
 */
class TimerStats(scheduler: ScheduledExecutorService) {

  def this() = this(Executors.newScheduledThreadPool(1))

  private val lastStat = new AtomicReference(TimerCounter(CTime.now))

  def onTimer(counter: TimerCounter, n: Long) {}

  scheduler.scheduleAtFixedRate(new Runnable {
    var counter = 0L

    override def run() {
      counter += 1

      val old = lastStat.getAndSet(TimerCounter(CTime.now))
      onTimer(old, counter)
    }
  }, 1000, 1000, TimeUnit.MILLISECONDS)

  def increment() = lastStat.get().count.incrementAndGet()

  def get() = lastStat.get().perSecond
}

case class TimerCounter(flushTime : Long, count : AtomicLong = new AtomicLong()) {
  def perSecond = {
    val c = count.get()
    val now = CTime.now
    (c / ( (now - flushTime) / 1000d )).toInt
  }
}
