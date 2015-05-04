package strd.util

import com.google.common.cache.Cache
import com.twitter.ostrich.stats.Stats
import com.twitter.util.Stopwatch
import strd.trace.PoolContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 10/3/13
 * Time: 3:41 PM
 */
object Stats2 {

  //implicit val executionContext = ExecutionContext.Implicits.global

  def futureTime[T](name: String)(f: Future[T])(implicit executionContext:ExecutionContext) : Future[T] = {
    val start = System.currentTimeMillis()
    f.onComplete{
      case Success(_) => addMetric(name, System.currentTimeMillis() - start toInt)
      case Failure(t) => addMetric(s"$name/err/${t.getClass.getName}", System.currentTimeMillis() - start toInt)
    }
    f
  }


  val stats = Stats

  def time[T](name: String)(f: => T) = {
    val start = CTime.now
    val rv = f
    val d = CTime.now - start
    addMetric(name, d.toInt)
    rv
  }

  def addMetric(name : String, d : Int) {
    PoolContext.traceOutput(1) {"metric:" + name +" := " + d + " msec"}
    Stats.addMetric(name + "_msec", d)
  }

  def addMetricPlain(name : String, d : Int) {
    PoolContext.traceOutput(1) {"metric:" + name +" := " + d }
    Stats.addMetric(name , d)
  }

  def incr(name: String) = {
    PoolContext.traceOutput(1) {"counter:" + name +" +1 "}
    stats.incr(name)
  }

  def incr(name: String, count: Int) = {
    PoolContext.traceOutput(1) {"counter:" + name +" +" + count}
    stats.incr(name, count)
  }

  def startTime(name: String) = StatsTimer(name, Stopwatch.start())

  def cacheStatsToMetrics(cache:Cache[_,_], name : String) {
    Stats.addGauge(s"$name/loadCount"){ cache.stats().loadCount() }
    Stats.addGauge(s"$name/size"){ cache.size() }
    Stats.addGauge(s"$name/hitCount"){ cache.stats().hitCount() }
    Stats.addGauge(s"$name/missCount"){ cache.stats().missCount() }
    Stats.addGauge(s"$name/hitRate"){ cache.stats().hitRate() }
    Stats.addGauge(s"$name/evictionCount"){ cache.stats().evictionCount() }
  }
}

case class StatsTimer(name: String, elapsed: Stopwatch.Elapsed) {
  def finish() = {
    Stats.addMetric(name, elapsed().inMilliseconds.toInt)
  }
}
