package strd.net.http

import com.twitter.ostrich.stats.Stats
import strd.util.Stats2

/**
 * @author Kirill chEbba Chebunin
 */
object HttpStats {
  val PREFIX = "http"

  def channels(c: => Int) = Stats.addGauge(s"$PREFIX/channels")(c)
  def rps(rps: => Int) = Stats.addGauge(s"$PREFIX/rps")(rps)
  def request() = Stats2.incr(s"$PREFIX/in")
  def response(r: HttpResp) {
    Stats2.incr(s"$PREFIX/out")
    Stats2.incr(s"$PREFIX/status/${r.status.code()}")
  }
  def time(t: Long) = Stats2.addMetricPlain(s"$PREFIX/time", t.toInt)
  def deadline() = Stats2.incr(s"$PREFIX/deadline")

  def handler(success: Boolean) {
    if (success) {
      Stats2.incr(s"$PREFIX/handler/success")
    } else {
      Stats2.incr(s"$PREFIX/handler/error")
    }
  }

  def unorderedReq(empty: Boolean = false) {
    if (empty) {
      Stats2.incr(s"$PREFIX/stream/unordered/empty")
    } else {
      Stats2.incr(s"$PREFIX/stream/unordered/drop")
    }
  }

  def queueTimeout() = Stats2.incr(s"$PREFIX/queue/offer_timeout")

  def keepAlive(count: Int, time: Long) {
    Stats2.addMetricPlain(s"$PREFIX/keep_alive/count", count)
    Stats2.addMetricPlain(s"$PREFIX/keep_alive/ms", time.toInt)
  }
}
