package strd.ostrich

import java.io.{IOException, OutputStreamWriter}
import java.net.Socket

import com.twitter.conversions.time._
import com.twitter.ostrich.admin.{AdminHttpService, PeriodicBackgroundProcess, StatsReporterFactory}
import com.twitter.ostrich.stats.{StatsCollection, StatsListener}
import com.twitter.util.Duration
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import strd.util.ClusterConfigs


/**
 *
 * User: light
 * Date: 06/05/14
 * Time: 16:19
 */

class GraphiteStatsFactory (period: Duration = 1.minute,
    serviceName: Option[String] = None,
    prefix: String = "unknown",
    host: String = "localhost",
    port: Int = 2013)
  extends StatsReporterFactory {

  def apply(collection: StatsCollection, admin: AdminHttpService) =
    new GraphiteStrdStatsLogger(host, port, period, prefix, serviceName, collection)

}

class GraphiteStrdStatsLogger(val host: String, val port: Int, val period: Duration, val prefix: String,
                          val serviceName: Option[String], collection: StatsCollection)
  extends PeriodicBackgroundProcess("GraphiteStatsLogger", period) {

  val logger = LoggerFactory.getLogger(getClass)

  val listener = new StatsListener(collection)
  //val shortHost =
  val hostname = ClusterConfigs.shortHostName
    //InetAddress.getLocalHost.getCanonicalHostName.takeWhile(x => x != '.')
  logger.info("Graphite host_prefix: " + hostname)

  val v1 = Array(":", "/", " ")
  val v2 = Array("_", ".", "_")

  def periodic() {
    try {
      val started = System.currentTimeMillis()
//      println("--->> START METRICS")
      write(new Socket(host, port))
      val spent = System.currentTimeMillis() - started
      if (spent > 100)
      println("Metrics appended : " + spent + " ms")
//      println("<<--- END METRICS")
    } catch {
      case e: IOException => logger.error("Error connecting to graphite: %s", e.getMessage)
    }
  }

  def write(sock: Socket) {
    val stats = listener.get()
    val statMap =
      stats.counters.map { case (key, value) => ("counter." + key, value.doubleValue) } ++
      stats.gauges.map { case (key, value) => ("gauge." + key, value) } ++
      stats.metrics.flatMap { case (key, distribution) =>
        distribution.toMap.map { case (subkey, value) =>
          ("metric." + key + "_" + subkey, value.doubleValue)
        }
      }
    val cleanedKeysStatMap = statMap.map { case (key, value) =>
      ( StringUtils.replace( StringUtils.replaceEach(key.toLowerCase, v1, v2), "..", "." ), value)
    }

    var writer: OutputStreamWriter = null
    try {
      val epoch = System.currentTimeMillis() / 1000

      writer = new OutputStreamWriter(sock.getOutputStream)

      try {
        cleanedKeysStatMap.foreach { case (key, value) =>
          val result = "%s.%s.%s %.2f %d\n".formatLocal(java.util.Locale.US, prefix, serviceName.getOrElse("unknown"), key, value.doubleValue, epoch)
//          println(result)
          writer.write(result)
        }
      } catch {
        case e: IOException => logger.error("Error writing data to graphite: %s", e.getMessage)
      }

      writer.flush()
    } catch {
      case e: Exception =>
        logger.error("Error writing to Graphite: %s", e.getMessage)
        if (writer != null) {
          try {
            writer.flush()
          } catch {
            case ioe: IOException =>
              logger.error("Error while flushing writer: %s", ioe.getMessage)
          }
        }
    } finally {
      if (sock != null) {
        try {
          sock.close()
        } catch {
          case ioe: IOException => logger.error("Error while closing socket: %s", ioe.getMessage)
        }
      }
      writer = null
    }
  }


}
