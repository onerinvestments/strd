package strd.net

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, TimeUnit, TimeoutException}
import java.util.{Timer, TimerTask}

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.google.protobuf.{Message, ProtoAccessor}
import com.twitter.ostrich.admin.Service
import com.twitter.ostrich.stats.Stats
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.AttributeKey
import lmbrd.io.JVMReservedMemory
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory
import strd.cluster.proto.StrdCluster._
import strd.trace.{PoolContext, RemoteTraceContext, TraceContext}
import strd.util.{CTime, ClusterConfigs, TimerCounter, TimerStats}

/**
 *
 * User: light
 * Date: 11/04/14
 * Time: 13:22
 */
object NettyConst {
  val attrKey = new AttributeKey[NodeConnection]( "con" )
}

class ClusterRegistration(val proto : StrdProto[_,_])(implicit val bindingModule: BindingModule) extends Injectable with Service {

  val log = LoggerFactory.getLogger(getClass)
  val meta       = inject[MetaProvider]

  val conf       = inject[ClusterConfigs]
  val curator    = inject[CuratorFramework]

  val group     = meta.meta.getGroupId
  val cluster   = ClusterConfigs.cluster.get
  val node      = meta.meta.getNodeId.toString

  val path      = s"/strd2/$cluster/$group"
  val selfPath  = s"$path/$node"

  val updateMetaTimer = new Timer(true)


  def checkParent() {
    if ( curator.checkExists().forPath(path) == null ) {
      log.info("Create initial path: " + path)
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
    }
  }

  checkParent()


  override def shutdown() = {

  }

  override def quiesce() = {
    try {
      updateMetaTimer.cancel()
      curator.delete().forPath(selfPath)
    }catch {
      case x: Exception =>
        log.warn("Self deregister failed", x)
    }
  }

  override def start() = {
    updateMetaTimer.scheduleAtFixedRate( new TimerTask() {
      override def run() = {
        try {
          val newMeta = meta.meta

          if (curator.checkExists().forPath(selfPath) == null || NodeMeta.parseFrom( curator.getData.forPath(selfPath), proto.extReg ) != newMeta) {
            checkParent()
            curator.create().withMode(CreateMode.EPHEMERAL).forPath(selfPath, meta.meta.toByteArray)
            log.debug("Self meta updated")
          }


        } catch {
          case x: Exception => log.error("Update node metaFailed", x)
        }
      }
    }, 3000, 1000)

  }

}

trait MetaProvider {
  def meta : NodeMeta
}

class SimpleMetaProvider(groupId  : String,
                         port     : Int,
                         nodeType : Int)(implicit val bindingModule: BindingModule) extends Injectable with MetaProvider {

  val conf          = inject[ClusterConfigs]
  val decommission  = new AtomicBoolean()

  override def meta = {
      NodeMeta.newBuilder()
        .setNodeId(conf.nodeId.get)
        .setGroupId(groupId)
        .setDecommission(decommission.get)
        .setHost(conf.publicAddress)
        .setHostString( conf.hostName )
        .setNodePort(port)
        .setNodeType(nodeType)
        .build()
  }
}

class StrdNodeServer(proto : StrdProto[_,_])(implicit val bindingModule: BindingModule) extends Injectable with Service  {
  val name      = proto.name
  val conf      = inject[ClusterConfigs]
  val handlers  = inject[StrdHandlers]
  val sp        = inject[MetaProvider]

  val port = sp.meta.getNodePort
  val log = LoggerFactory.getLogger(getClass)

  val bossGroup = new NioEventLoopGroup(1, PoolContext.threadFactory(proto.name + "_boss", Thread.MAX_PRIORITY - 1))
  val workerGroup = new NioEventLoopGroup( (Runtime.getRuntime.availableProcessors() / 2 ) + 1 , PoolContext.threadFactory(proto.name + "_work", Thread.MAX_PRIORITY - 2)  )

  if (JVMReservedMemory.getMaxMemory < 64 * 1024 * 1024) {
    log.error("Potential problem: MAX_DIRECT_MEM: " + (JVMReservedMemory.getMaxMemory / 1024) + "kb")
  }


  val rps = new TimerStats(bossGroup) {
    override def onTimer(counter: TimerCounter, n: Long) {
      if (n % 20 == 0) {
        log.debug(s"Server $name RPS: ${counter.perSecond}")
      }
    }
  }

  Stats.addGauge(s"server/$name/RPS") {
    rps.get()
  }


  override def shutdown() = {

  }

  override def quiesce() = {

    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()

    bossGroup.terminationFuture().sync()
    workerGroup.terminationFuture().sync()

  }

  override def start() = {
    val boot = new ServerBootstrap()
        boot.group(bossGroup, workerGroup)
          .channel(classOf[NioServerSocketChannel])
          .childHandler(new ProtobufChannelInitializer(proto, () => new StrdClusterServerHandler(handlers, sp.meta.getNodeId, this)))
          .option(ChannelOption.SO_BACKLOG, new Integer(16000))
        boot.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, new Integer(10 * 64 * 1024) )
        boot.option(ChannelOption.SO_SNDBUF,    new Integer(1048576) )
        boot.option(ChannelOption.SO_RCVBUF,    new Integer(1048576) )
        boot.option(ChannelOption.TCP_NODELAY,  new java.lang.Boolean(true) )

        boot.bind(port).sync()

    log.debug("Served bound " + port)
  }
}

class StrdClusterServerHandler(val handlers : StrdHandlers,
                               val nodeId : Int,
                               val server : StrdNodeServer) extends SimpleChannelInboundHandler[Message]{

  var ctx   : Option[ChannelHandlerContext] = None

  val log = LoggerFactory.getLogger(getClass)

  private val responses = new ArrayBlockingQueue[ NodeResponse ](1024)
  private val scheduledWrite = new AtomicBoolean()

  def appendResponse(resp : NodeResponse) {
    if( ! responses.offer( resp, 3, TimeUnit.SECONDS )) {
      throw new TimeoutException("can not submit response")
    }

    if (scheduledWrite.compareAndSet(false, true)) {
      ctx.get.executor().execute( flushQueue )
    }
  }

  val flushQueue = new Runnable {
    override def run() = {
      scheduledWrite.set(false)
      try {
        val ar = new java.util.ArrayList[NodeResponse](12)
        responses.drainTo(ar)
        val c = ctx.get
        val iter = ar.iterator()

        while (iter.hasNext) {
          c.write(iter.next()).addListener(channelWriteListener)
        }
        c.flush()
      } catch {
        case x:Exception => log.error("Failed", x)
      }
    }
  }

  override def channelRegistered(ctx: ChannelHandlerContext) = {
    super.channelRegistered(ctx)
    log.debug("Channel registered: " + ctx.channel().remoteAddress())
  }

  override def channelUnregistered(ctx: ChannelHandlerContext) = {
    super.channelUnregistered(ctx)

    this.ctx = None
    log.debug("Channel unregistered: " + ctx.channel().remoteAddress())
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) = {
//    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    log.error("Unexpected exception", cause)
  }

  val channelWriteListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture) = {
      if ( future.cause() != null ) {
        log.error("Unexpected write exception", future.cause())
      }
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message) = {
    msg match {
      case x : HandshakeRequest =>
        try {
/*
          val scheduledFuture = ctx.executor().scheduleAtFixedRate(new Runnable() {
            override def run() = {
              purge()
            }
          }, 2, 2, TimeUnit.MILLISECONDS)
*/

//          this.timer = Some(scheduledFuture)
          this.ctx = Some(ctx)

          ctx.attr(NettyConst.attrKey).set(RemoteNodeConnection(x.getNodeId, this))

          ctx.writeAndFlush(HandshakeResponse.newBuilder()
            .setReqTimestamp(x.getTimestamp)
            .setTimestamp(System.currentTimeMillis())
            .setNodeId(nodeId)
            .build())
//          println("Handshake written")
        } catch {
          case x:Exception => log.error("failed", x)
        }

      case x : NodeRequest =>
        appendStat()

        val con = ctx.attr( NettyConst.attrKey ).get()
        if (con == null) {
          throw new IllegalStateException("no nodeConnection in context")
        }

        val traceRoot =
          if ( x.hasTrace && x.getTrace.getTraceLevel >= 1) {         // wanna some trace
            RemoteTraceContext.startFromRemote( server.conf, x, con )
          } else {
            None
          }

        handlers.handleRequest(x, con, traceRoot)
    }
  }

  def appendStat() {
    server.rps.increment()
  }
}

class StrdHandlers( pp : StrdProto[NodeRequest,NodeResponse]) {
  case class Handler[X <: Message](handler: (X, RequestContext) => Unit)

  val handlers = new ConcurrentHashMap[Class[_], Handler[_ <: Message] ]()

  def addHandler[X <: Message](f: (X, RequestContext) => Unit)(implicit m: Manifest[X]) = {
    val h = Handler(f)
    handlers.put(m.runtimeClass, h)
    this
  }

  def defaultPing( req : PingRequest, ctx : RequestContext) {
    ctx ! PingResponse.newBuilder().setTime(req.getTime).build()
  }

  addHandler(defaultPing)

  def handleRequest( req : NodeRequest, con : NodeConnection, traceRoot : Option[TraceContext] ) {
    val startTime = CTime.now

    val ext = ProtoAccessor.getExtension(req)

    val h = handlers.get(ext.getClass)
    val reqName = ext.getClass.getSimpleName
    val metricName = s"request/${pp.name}/$reqName"
    Stats.incr(metricName)
//    println
    if (h == null) {
      throw new IllegalStateException("no handler for request:" + ext)
    } else {
      val ctx = RequestContextImpl(pp, con, req, startTime, metricName, traceRoot)
//      println("CTX:  " + traceRoot)
      traceRoot.map( t => PoolContext.threadLocal.set(t) )
      try {
        h.asInstanceOf[Handler[Message]].handler(ext.asInstanceOf[Message], ctx)
      } finally {
        PoolContext.clear()
      }
    }
  }
}

// incoming connection
trait NodeConnection {

  def nodeId : Int

  def send(msg : NodeResponse)

}

case class RemoteNodeConnection(nodeId : Int,
                                queue  : StrdClusterServerHandler) extends NodeConnection {

  override def send(msg: NodeResponse) = {
    queue.appendResponse(msg)
  }

}

trait RequestContext {
  def completed : Boolean
  def srcReq : NodeRequest
  def !!(failure : Exception)
  def ![X <: Message](msg: X)
  def nodeId : Int
  def discard : Unit
}

case class RequestContextImpl(proto   : StrdProto[NodeRequest,NodeResponse],
                              con     : NodeConnection,
                              srcReq  : NodeRequest,
                            startTime : Long,
                              metric  : String,
                             traceRoot: Option[TraceContext]) extends RequestContext {

  var completed = false

  override def nodeId = con.nodeId

  override def discard = {
    traceRoot.map {
      trace =>
        trace.appendMessage("Context was discarded")
        trace.submitResult()
    }
  }

  def !!(failure : Exception) {
    try {
      val builder = RFail.newBuilder()
        .setErrorCode(ErrorCode.APP_EXCEPTION)
        .setMsg(failure.getClass.getName + ":" + failure.getMessage)

      failure.getStackTrace.foreach(ste =>
        builder.addStackTrace(StackTraceElement.newBuilder()
          .setDeclaringClass(ste.getClassName)
          .setFileName(ste.getFileName)
          .setMethodName(ste.getMethodName)
          .setLineNumber(ste.getLineNumber))
      )

      if (completed )
        return

      this ! builder.build()
    } catch {
      case ignored : Exception => ignored.printStackTrace()
    }
  }

  /**
   * Write response
   * @param msg response payload
   * @tparam X type of payload
   */
  def ![X <: Message](msg: X) = {
    synchronized {
      if (completed) throw new IllegalStateException("response was already sent")

      traceRoot.map{ trace =>
        trace.appendMessage("Reply:" + msg.getClass.getSimpleName +"\n" + msg)
        trace.submitReply(msg)
      }

      val response = proto.createResponse(msg).asInstanceOf[NodeResponse.Builder]
      response.setReqId(srcReq.getReqId)
//      println("Send response: " + response)
      val build = response.build()

      completed = true
      con.send(build)
    }

    Stats.addMetric(metric, (CTime.now - startTime).toInt )
  }

}

