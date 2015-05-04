package strd.trace

import ch.qos.logback.classic
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.spi.FilterReply
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.google.protobuf.Message
import com.twitter.ostrich.admin.Service
import org.slf4j.{Logger, LoggerFactory}
import strd.cluster.proto.StrdCluster
import strd.cluster.proto.StrdCluster.{NodeMeta, NodeRequest, RequestTrace, TraceAppend}
import strd.net.{ClusterConnector, NodeConnection, StrdProtos}
import strd.util.{CTime, ClusterConfigs, ClusterStrdConf, StrdService}

import scala.util.Random

/**
 *
 * User: light
 * Date: 13/05/14
 * Time: 18:46
 */

object RemoteTraceContext {

  var globalTrace : Option[String] = None
  var globalTraceLevel : Int = 2

/*
  def startTrace(name: String, level : Int): Option[TraceContext] = {
    PoolContext.traceSubmmiter.map( s=> {
      val ctx = TraceContext(s, TraceEntry(name, "", CTime.now), level)
      PoolContext.threadLocal.set(ctx)
      ctx
    } )
  }

  def stopTrace(ctx : TraceContext) {
    ctx.submitResult()
    PoolContext.threadLocal.remove()
  }

  def stopTrace(future : Future[_], ctx : TraceContext) {
    PoolContext.futureStopTrace(future, ctx)
    PoolContext.threadLocal.remove()
  }
*/

  def remoteRequest(builder: NodeRequest.Builder, payload: Message, timeout: Long, meta : NodeMeta) = {
    PoolContext.traceSubmmiter.map( s=> {
      val ctx = PoolContext.threadLocal.get()

      if (ctx != null) {
        ctx.appendMessage(s"-> remote req:${meta.getGroupId}@${meta.getHostString}(${meta.getNodeId}) deadline: ${builder.getReqDeadline}   timeout: $timeout  msg: ${payload.getClass.getSimpleName} "  )

        val trace = RequestTrace.newBuilder()
        trace.setPath(ctx.trace.fullPath + "#" + ctx.remoteInvoke )
        trace.setTraceLevel(ctx.traceLevel)
        trace.setRequestSent(CTime.now)

        builder.setTrace(trace)
      }
    } )
  }


  def startFromRemote( conf : ClusterConfigs,
                       request: NodeRequest,
                       connection: NodeConnection) : Option[TraceContext] = {

    PoolContext.traceSubmmiter.map( s=> {
      val trace = request.getTrace
      val now = CTime.now

      val root = TraceEntry( s"${conf.nodeServiceId}@${conf.hostName}(${conf.nodeId.get})", trace.getPath, now)

      val untilDeadline = if (request.hasReqDeadline) (request.getReqDeadline - now).toString else "-"
      root.append(s"rcvd networkTime: ${now - trace.getRequestSent}, fromNodeId: ${connection.nodeId}, untilDeadline: $untilDeadline", now )

      TraceContext(s, root, trace.getTraceLevel)
    })
  }

}

class NarkClient(implicit val bindingModule: BindingModule) extends Injectable with Service {

  val protos        = StrdProtos.build("nark_client", classOf[StrdCluster])
  val connector     = new ClusterConnector( protos, "nark" )
  val log           = LoggerFactory.getLogger(getClass)
  override def shutdown() = {
    PoolContext.traceSubmmiter = None
    connector.shutdown()
  }

  override def start() = {
    connector.start()
    PoolContext.traceSubmmiter = Some( new TraceSubmmiterImpl(connector) )
    log.info("-- Nark is initialized")
  }

}

trait NarkStrdConf { self : StrdService
                            with ClusterStrdConf =>

  bind[NarkClient] toSingle new NarkClient()

  def narkLogAppender = {
    val appender = new TraceLogAppender

    val filter = new ThresholdFilter {
      override def decide(event: ILoggingEvent) = {
        if ( PoolContext.threadLocal.get() == null)
          FilterReply.DENY
        else
          super.decide(event)
      }
    }

    filter.setLevel(Level.DEBUG.levelStr)
    filter.start()

    appender.addFilter(filter)
    appender.start()

    appender
  }

  LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).asInstanceOf[classic.Logger].addAppender(narkLogAppender)
}

class TraceSubmmiterImpl(cluster :ClusterConnector) extends TraceSubmitter{
  val log = LoggerFactory.getLogger( getClass )

  override def appendTrace(traceRoot: TraceEntry) {
    val connectors = cluster.getGroup
    if (connectors.isEmpty) {
      log.warn("No connectors to trace service")
      return
    }

    try {
      val builder = TraceAppend.newBuilder()
      val traces = traceToEntry(traceRoot)
      traces.foreach(builder.addTraces)
      builder.setTimeGlobal( traceRoot.reqStarted )

     /* val message = NodeRequest.newBuilder()
        .setReqId(CTime.now)
        .setExtension(TraceAppend.traceAppend, builder.build())
        .build()*/
//      println("Submit trace: " + traces.mkString("\n"))
      connectors(Random.nextInt(connectors.size)).sendWithoutReply(builder.build())
    } catch {
      case x:Exception => log.warn("Trace submission failed", x)
    }
  }


  def traceToEntry( t : TraceEntry ) : Seq[StrdCluster.TraceEntry] = {
    val builder = StrdCluster.TraceEntry.newBuilder()

    builder.setPath( t.fullPath )

    t.msgs.foreach( m => builder.addMessagesBuilder().setLocalTime(m.timeLocal).setText(m.msg) )

    val THIS = builder.build()

    THIS +: t.children.flatMap( c => traceToEntry(c) )
  }
}
