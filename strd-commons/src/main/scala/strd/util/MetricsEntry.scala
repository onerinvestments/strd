package strd.util

import lmbrd.zn.util.TimeUtil.TimeGroupType
import com.twitter.ostrich.stats.Histogram
import play.api.libs.json.Json

/**
 * Created by zwie on 07.04.14.
 */
case class MetricsHistogramJson(count: Long, sum: Long, buckets: Array[Long])

case class MetricsEntryJson(date: Long, metricName: String, server: String, nodeName: String, count: Long, gauge: Long, h: MetricsHistogramJson)

object MetricsHistogramJson {
  implicit val metricsHistogramJson = Json.format[MetricsHistogramJson]
}

object MetricsEntryJson {

  def toJson(entry: MetricsEntry) = {
    MetricsEntryJson(entry.date, entry.metricName, entry.server, entry.nodeName, entry.count.longValue(), entry.gauge.longValue(), MetricsHistogramJson(entry.h.count, entry.h.sum, entry.h.buckets))
  }

  implicit val metricsEntryJson = Json.format[MetricsEntryJson]
}

case class MetricsNamesReq()
case class MetricsForNameReq(metricName: String, from: Long, to: Long, tg: TimeGroupType)
case class MetricsEntry(date: Long, metricName: String, server: String, nodeName: String, count: Number, gauge: Number, h: Histogram)
