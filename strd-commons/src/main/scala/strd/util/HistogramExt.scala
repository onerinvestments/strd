package strd.util

import com.twitter.ostrich.stats.Histogram

/**
 *
 * User: light
 * Date: 8/7/13
 * Time: 4:55 PM
 */

class HistogramExt extends Histogram {

  def time[X](h: => X):X = {
    val begin = System.currentTimeMillis()
    val r = h
    addNoLock( (System.currentTimeMillis() - begin).toInt )
    r
  }

  def addNoLock(n:Int) {
    val index = Histogram.bucketIndex(n)

    addToBucket(index)
    sum += n
  }

  override def toString() = {
    f"min:$minimum%15d    max:$maximum%,12d   count:$count%,12d    sum:$sum%,12d  50p:${getPercentile(0.5d)}%,12d    75p:${getPercentile(0.75d) }%,12d     90p:${getPercentile(0.9d) }%,12d    99p:${getPercentile(0.99d) }%,12d"
  }
}
