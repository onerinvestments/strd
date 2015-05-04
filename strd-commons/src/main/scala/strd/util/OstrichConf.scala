package strd.util

import ch.qos.logback.classic
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.twitter.ostrich.admin._
import com.twitter.ostrich.stats._
import com.twitter.util.Duration
import lmbrd.zn.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import strd.ostrich.GraphiteStatsFactory

class OstrichConfParams extends ConfigParamsDeclaration {
  val GRAPHITE_HOST   = StringParam       ("graphite.host")
  val GRAPHITE_PORT   = IntegerParam      ("graphite.port",   defaultValue = 2003)
  val GRAPHITE_PERIOD = TimeIntervalParam ("graphite.period", defaultValue = TimeUtil.aSECOND * 20)

  // predefined ostrich ports
  val DHT3_OSTRICH_PORT         = IntegerParam  ("dht3_node.ostrich.http",        defaultValue = 1881, minimal = 8000, maximal = 64000)
  val DSP_OSTRICH_PORT          = IntegerParam  ("dsp.ostrich.http",              defaultValue = 8862, minimal = 8000, maximal = 64000)
  val DSP_FRONTEND_OSTRICH_PORT = IntegerParam  ("dsp-frontend.ostrich.http",     defaultValue = 1883, minimal = 8000, maximal = 64000)
  val DSP_GATEWAY_OSTRICH_PORT  = IntegerParam  ("dsp-gateway.ostrich.http",      defaultValue = 8863, minimal = 8000, maximal = 64000)
  val TD_API_OSTRICH_PORT       = IntegerParam  ("td-api-server.ostrich.http",    defaultValue = 8866, minimal = 8000, maximal = 64000)
  val RTB_PROXY_OSTRICH_PORT    = IntegerParam  ("rtb-proxy-server.ostrich.http", defaultValue = 1882, minimal = 8000, maximal = 64000)

  val UTL_ROTATOR_OSTRICH_PORT  = IntegerParam  ("utl-rotator.ostrich.http",      defaultValue = 8860, minimal = 8000, maximal = 64000)
  val ADC_SSP_OSTRICH_PORT      = IntegerParam  ("adc-ssp.ostrich.http",          defaultValue = 1884, minimal = 8000, maximal = 64000)

  val UTL_GATEWAY_OSTRICH_PORT  = IntegerParam  ("utl-gateway.ostrich.http",      defaultValue = 8859, minimal = 8000, maximal = 64000)
  val UTL_TOOLBAR_OSTRICH_PORT  = IntegerParam  ("utl-toolbar.ostrich.http",      defaultValue = 8840, minimal = 8000, maximal = 64000)
  val DATANODE_OSTRICH_PORT     = IntegerParam  ("datanode.ostrich.http",         defaultValue = 8858, minimal = 8000, maximal = 64000)
  val DHTNODE_OSTRICH_PORT      = IntegerParam  ("dht-node.ostrich.http",         defaultValue = 8861, minimal = 8000, maximal = 64000)
  val NARK_OSTRICH_PORT         = IntegerParam  ("nark.ostrich.http",             defaultValue = 1880, minimal = 8000, maximal = 64000)
  val WATCH_DOG_OSTRICH_PORT    = IntegerParam  ("watch-dog.ostrich.http",        defaultValue = 8870, minimal = 8000, maximal = 64000)
  val KAFKA_OSTRICH_PORT        = IntegerParam  ("kafka.ostrich.http",            defaultValue = 8871, minimal = 8000, maximal = 64000)
  val DPI_OSTRICH_PORT          = IntegerParam  ("dpi.ostrich.http",              defaultValue = 8872, minimal = 8000, maximal = 64000)
  val SSP_OSTRICH_PORT          = IntegerParam  ("ssp.ostrich.http",              defaultValue = 1885, minimal = 8000, maximal = 64000)


}

object OstrichConf extends OstrichConfParams

trait OstrichStrdConf { this: StrdService with ClusterStrdConf =>
  def defaultOstrichPort : Int = 0

  def nodePort =
    conf.props.find(_._1 == clusterNodeName + ".ostrich.http").map(_._2.toInt).getOrElse( defaultOstrichPort )

  def metricLogAppender = {
    val appender = new MetricLogAppender

    val filter = new ThresholdFilter
    filter.setLevel(Level.WARN.levelStr)
    filter.start()

    appender.addFilter(filter)
    appender.start()

    appender
  }

  LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).asInstanceOf[classic.Logger].addAppender(metricLogAppender)

  val ostrichAdmin = {
    val port = nodePort
    val logger = LoggerFactory.getLogger( getClass )

    logger.info(s"--> OSTRICH HttpService, port_conf:" + port)
    val reporters = if(!ClusterConfigs.cluster.exists(x=> x == "uptolike" || x == "adcamp")) {
      logger.warn("Graphite metrics disabled cluster name is not uptolike|adcamp")
      Nil
    } else if (conf.props.contains(OstrichConf.GRAPHITE_HOST.id)) {
      val shortHost = ClusterConfigs.shortHostName
      val serviceName = s"${ClusterConfigs.cluster.get}.${conf.nodeServiceId}.${shortHost.replaceAll("[/.]", "_")}_${conf.nodeId.get}"
      logger.info("Graphite service_name: " + serviceName)

      List(new GraphiteStatsFactory(
        host = OstrichConf(_.GRAPHITE_HOST),
        port = OstrichConf(_.GRAPHITE_PORT),
        prefix = "strd",
        serviceName = serviceName,
        period = Duration.fromMilliseconds(OstrichConf(_.GRAPHITE_PERIOD))
      ))
    } else {
      logger.warn("Graphite metrics disabled, no :" + OstrichConf.GRAPHITE_HOST.id + " prop found")
      Nil
    }

    val admin = AdminServiceFactory( port, statsNodes = List(
      new StatsFactory(reporters = reporters)
    ))(RuntimeEnvironment(this, Array.empty[String]))

    conf.nodeId.foreach(nodeId =>
      Stats.setLabel("cluster/node_id", conf.nodeId.toString)
    )
    ClusterConfigs.cluster.foreach(cluster =>
      Stats.setLabel("cluster/clusterName", cluster)
    )
    Stats.setLabel("cluster/nodeType", clusterNodeName)

    ServiceTracker.register( admin )

    logger.info("\nOstrich listen on " + admin.get.address.getPort +" port\n")
    admin
  }
}

class MetricLogAppender extends AppenderBase[ILoggingEvent] {
  def append(eventObject: ILoggingEvent) {
    val counter = s"log/${eventObject.getLevel.levelStr.toLowerCase}/${StringUtils.replace( eventObject.getLoggerName, "." ,"_" )}"
    Stats.incr(counter)
  }
}
