package strd.net

import java.net.InetSocketAddress
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.google.protobuf.Message
import com.twitter.ostrich.admin.Service
import com.twitter.ostrich.stats.Stats
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import lmbrd.io.JVMReservedMemory
import lmbrd.zn.util.{TimeSuppressor, TimeUtil}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory
import strd.cluster.proto.StrdCluster._
import strd.dht.BIN
import strd.trace.{PoolContext, RemoteTraceContext}
import strd.util.{CTime, ClusterConfigs, StrdNodeConf}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}


class ClusterConnector(val pp: StrdProto[NodeRequest, NodeResponse], val group: String)(implicit val bindingModule: BindingModule) extends Injectable with Service {
  val timeoutScheduler = Executors.newScheduledThreadPool(2, PoolContext.threadFactory("con-timeout", Thread.MAX_PRIORITY - 1))

  val log = LoggerFactory.getLogger(getClass)

  if (JVMReservedMemory.getMaxMemory < 64 * 1024 * 1024) {
    log.error("Potential problem: MAX_DIRECT_MEM: " + (JVMReservedMemory.getMaxMemory / 1024) + "kb")
  }

  val conf = inject[ClusterConfigs]
  val curator = inject[CuratorFramework]

  val maxTimeDiff = StrdNodeConf(_.CONNECTOR_TIME_DIFF)(conf)

  val path = s"/strd2/${ClusterConfigs.cluster.get}/$group"

  if ( curator.checkExists().forPath(path) == null ) {
    log.info("Create initial path: " + path)
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
  }

  /*
    var nodesSeq  = Seq.empty[NodeConnector]
    var nodesMap  = Map.empty[Int,NodeConnector]
    val lock      = new ReentrantReadWriteLock()
  */

  val zkListener = new ZkPathWithPurge[NodeMeta, NodeConnector](curator, path) {

    /*override def serialize(msg: NodeConnector) = {
      msg.meta.toByteArray
    }

    override def deserialize(bytes: BIN) = {
      val meta = parseMeta(bytes)
      log.info("Adding node: " + meta)
      new MultiplexingNodeConnector(3, meta, ClusterConnector.this, pp)
    }
*/
    override def deserialize(bytes: BIN) = {
      parseMeta(bytes)
    }


    override def metaForValue(s: NodeConnector) = {
      s.meta
    }

    override def serialize(msg: NodeMeta) = {
      msg.toByteArray
    }

    override def onUpdated(msg: NodeMeta, path: String, entry: NodeConnector) = {
      log.info("Update node: " + msg.getNodeId)
      val oldMeta = entry.meta
      entry.updateMeta(msg)

      entry
    }

    override def onAdded(msg: NodeMeta, path: String) = {
      log.info("Adding node: " + msg.getNodeId)
      val con = new MultiplexingNodeConnector(3, msg, ClusterConnector.this, pp, maxTimeDiff)

      con
    }

    override def onRemoved(entry: NodeConnector, path: String) = {
      log.info("Removing node: " + entry.meta.getNodeId)
      entry.stop()

    }

    override def onShutdown(entry: NodeConnector, path: String) = {
      log.info("Shutdown node: " + entry.meta.getNodeId)
      entry.stop()

    }

    override def afterStateChanged(state: Seq[NodeConnector]) = {
      invokeListener {
        onClusterChanged(state)
      }
    }
  }

  def removeNode(connector: NodeConnector) {
    zkListener.removeEntryImpl(connector.meta.getNodeId.toString)
  }

  private def invokeListener(f: => Unit) {
    try {
      f
    } catch {
      case ignored: Exception => log.error("Listener failed", ignored)
    }
  }

  /*
    if (curator.checkExists().forPath(path) == null) {
      log.info("Create initial path: " + path)
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
    }
  */

  override def start() = {
    zkListener.start()
    // wait connection
  }

  def parseMeta(bytes: Array[Byte]): NodeMeta = NodeMeta.parseFrom(bytes, pp.extReg)

  def nodeConnected(connector: NodeConnector) = {
    log.info(s"Cluster Node registered : ${connector.meta.getNodeId} ")
  }

  def nodeDisconnected(connector: NodeConnector) = {
    log.info(s"Cluster Node unregistered : ${connector.meta.getNodeId} ")
  }

  def getNode(id: Int): Option[NodeConnector] = {
    zkListener.state.get().seq.find(_.meta.getNodeId == id)
  }

  def getGroup: Seq[NodeConnector] = {
    zkListener.state.get().seq
  }

  def broadcast[X](msg: Message, timeout: Long)(implicit executionContext: ExecutionContext): Future[Seq[X]] = {
    Future.sequence(getGroup.map(c => c.send[X](msg, timeout)))
  }

  override def shutdown() = {
    log.info("Stopping cluster connector")
    zkListener.shutdown()
  }

  override def quiesce() = {
  }

  protected def onClusterChanged(state: Seq[NodeConnector]) {}

  /*
    protected def onNodeAdded( nc : NodeConnector ) {}
    protected def onNodeRemoved( nc : NodeConnector ) {}
    protected def onMetaUpdated( oldMeta : NodeMeta, newMeta : NodeMeta, nc : NodeConnector ) {}
  */
}


case class ClusterState(nodes: Map[Int, NodeConnector])

// outgoing connection
trait NodeConnector {
  def stop()

  def meta: NodeMeta

  def updateMeta(meta: NodeMeta)

  def sendReq[X](msg: NodeRequest): Future[X]

  def send[X](msg: Message, timeout: Long = 3000, traceRequest: Option[RequestTrace] = None): Future[X]

  def sendWithoutReply(msg: Message)
}

class MultiplexingNodeConnector(count: Int = 3,
                                val meta: NodeMeta,
                                val dispatcher: ClusterConnector,
                                val pp: StrdProto[NodeRequest, NodeResponse],
                                maxTimeDiff: Integer = 100) extends NodeConnector {
  val nodes = (0).until(count).map(i => {
    val cn = new RemoteNodeConnector(meta, dispatcher, pp, maxTimeDiff)
    cn.doConnect()
    cn
  }).toIndexedSeq

  def node = nodes(Random.nextInt(count))

  override def send[X](msg: Message, timeout: Long, traceRequest: Option[RequestTrace]) = {
    node.send[X](msg, timeout, traceRequest)
  }

  override def sendWithoutReply(msg: Message) {
    node.sendWithoutReply(msg)
  }

  override def sendReq[X](msg: NodeRequest) = {
    node.sendReq[X](msg)
  }

  override def updateMeta(meta: NodeMeta) = {
    nodes.foreach(_.updateMeta(meta))
  }

  override def stop() = {
    nodes.foreach(_.stop())
  }
}

case class RequestMade(reqId: Long,
                       deadline: Long,
                       p: Promise[_],
                       timeoutFuture: java.util.concurrent.ScheduledFuture[_])

case class RequestSubmitted(req: NodeRequest, p: Option[Promise[_]])

@Sharable
class RemoteNodeConnector(var meta: NodeMeta,
                          val dispatcher: ClusterConnector,
                          val pp: StrdProto[NodeRequest, NodeResponse],
                          maxTimeDiff: Integer = 100) extends SimpleChannelInboundHandler[AnyRef] with NodeConnector {

  override def updateMeta(meta: NodeMeta) = {
    this.meta = meta
  }

  val addr = new InetSocketAddress(meta.getHost, meta.getNodePort)

  val log = LoggerFactory.getLogger(s"${getClass.getName}#${meta.getGroupId}-${meta.getNodeId}/${meta.getHost}:${meta.getNodePort}")

  val DEFAULT_TIMEOUT = TimeUtil.aMINUTE
  val MAX_REQ_IN_PROGRESS = 2000
  // without reply
  val CONNECT_TIMEOUT = 2000


  val reqCounter = new AtomicLong()

  private val reqMap = new ConcurrentHashMap[Long, RequestMade]()

  val shutdown = new AtomicBoolean(false)
  var ctx: Option[ChannelHandlerContext] = None

  val boot = new Bootstrap
  val workerGroup = new NioEventLoopGroup(1)
  boot.group(workerGroup)
  boot.channel(classOf[NioSocketChannel])
  boot.handler(new ProtobufChannelInitializer(pp, () => this))

//  boot.option(ChannelOption.SO_SNDBUF,    new Integer(1048576) )
//  boot.option(ChannelOption.SO_RCVBUF,    new Integer(1048576) )
  boot.option(ChannelOption.TCP_NODELAY,  new java.lang.Boolean(true) )

  def doConnect(retry: Int = 0) {
    log.debug(s"Connecting to $addr...")
    val connectFuture = boot.connect(addr)
    val cancelWork = createCancelTask(connectFuture, retry)
    this.scheduledFuture = Some(workerGroup.schedule(cancelWork, CONNECT_TIMEOUT, TimeUnit.MILLISECONDS))
  }

  def createCancelTask(connectFuture: ChannelFuture, retry: Int) = {
    new Runnable {
      override def run() = {
        log.warn(s"Connecting to $addr timed out (try:$retry)")

        try {
          connectFuture.cancel(true)
        } catch {
          case x: Exception => log.error("Failed", x)
        }
        if (!shutdown.get()) {
          doConnect(retry + 1)
        }
      }
    }
  }

  //  var shouldFlush = false
  var scheduledFuture: Option[ScheduledFuture[_]] = None
  var handshakeFuture: Option[ScheduledFuture[_]] = None
  val hostName = addr.getHostString
  val reqTimeoutMetricName = s"connector/${pp.name}/$hostName/req_timeout"

  private val scheduledWrite = new AtomicBoolean()

  val channelWriteListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture) = {
      if (future.cause() != null) {
        log.error("Unexpected write exception", future.cause())
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    super.exceptionCaught(ctx, cause)
    log.error("Unexpected exception", cause)
  }

  override def channelActive(ctx: ChannelHandlerContext) = {
    super.channelActive(ctx)

    log.debug("Channel active: " + ctx.channel().remoteAddress())
    scheduledFuture.map(_.cancel(true))
    scheduledFuture = None

    if (handshakeFuture.isDefined) {
      log.warn("Channel active, but already have handshakeFuture")
      throw new RuntimeException("Channel active, but already have handshakeFuture")
    }

    if (shutdown.get()) {
      log.warn("channel registered, but shutdown -> ignore")
      ctx.close()
    } else {
      handshakeFuture = Some(
        workerGroup.schedule(new Runnable() {
          override def run() = {

            log.warn(s"Server $meta did not respond to handshake, close connect, " + handshakeFuture.get.hashCode())
            ctx.close()
          }
        }, 3, TimeUnit.SECONDS))

      ctx.writeAndFlush(
        HandshakeRequest.newBuilder
          .setNodeId(dispatcher.conf.nodeId.get)
          .setTimestamp(CTime.now).build()).addListener(channelWriteListener)
    }
  }

  override def channelRegistered(ctx: ChannelHandlerContext) = {
    super.channelRegistered(ctx)

    log.debug(s"Channel registered ${ctx.channel().remoteAddress()}")
  }

  var nodeRegistration: Option[Boolean] = None

  def registerNode(ctx: ChannelHandlerContext) {
    this.ctx = Some(ctx)

    nodeRegistration = Some(true)
    dispatcher.nodeConnected(this)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    super.channelInactive(ctx)
    log.debug("Channel inactive: " + ctx.channel().remoteAddress())
  }

  override def channelUnregistered(ctx: ChannelHandlerContext) = {
    super.channelUnregistered(ctx)
    log.warn(s"Connection lost to $addr")

    cancelScheduled()

    this.ctx = None


    nodeRegistration.foreach {
      x => dispatcher.nodeDisconnected(this)
    }
    nodeRegistration = None

    try {
      ctx.close( )
    } catch {
      case x:Exception =>
        log.warn("Exception while closing context",x )
    }

    if (!shutdown.get()) {
      log.debug("Schedule reconnect")
      workerGroup.schedule(new Runnable {
        override def run() = {
          doConnect()
        }
      }, 3, TimeUnit.SECONDS)

    }
  }

  def cancelScheduled() {
    scheduledFuture.map(_.cancel(true))
    scheduledFuture = None
    handshakeFuture.map(x => {
      x.cancel(true)
    })
    handshakeFuture = None


  }

  def stop() {
    cancelScheduled()

    log.info(s"Stop node connector: ${meta.getNodeId} / $addr")
    shutdown.set(true)

    try {
      dispatcher.removeNode(this)
    } catch {
      case ignored: Exception =>
    }

    try {
      ctx.map(_.close())
    } catch {
      case ignored: Exception =>
    }

    workerGroup.shutdownGracefully()
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) = {
    super.channelReadComplete(ctx)
    ctx.flush()
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: AnyRef) = {
    try {
      channelRead00(ctx, msg)
    } catch {
      case x : Exception =>
        if ( TimeSuppressor.can(pp.name + "_channelRead0", TimeUtil.aSECOND * 2) ) {
          log.error("Unexpected exception", x)
        }
    }
  }

  private def channelRead00(ctx: ChannelHandlerContext, msg: AnyRef) = {

    msg match {
      case h: HandshakeResponse =>

        cancelScheduled()

        val time = CTime.now
        val doublePing = time - h.getReqTimestamp
        val timeDiff = math.abs(time - h.getTimestamp) - doublePing / 2d

        log.debug(s"Time diff on servers: $timeDiff doublePing: $doublePing server: ${ctx.channel().remoteAddress()} nodeId: ${meta.getNodeId}")

        if (meta.getNodeId != h.getNodeId) {
          log.error(s"Stopping connector, due to nodeId mismatch: $h expected: ${meta.getNodeId}")
          stop()

        } else if (math.abs(timeDiff) > maxTimeDiff) {
          log.error(s"Stopping connector, due to big timeDiff: $timeDiff > $maxTimeDiff")
          stop()

        } else {
          registerNode(ctx)
        }

      case r: NodeResponse =>
        //        inPogress.decrementAndGet()
        val req = reqMap.remove(r.getReqId)
        if (req != null) {
          try {
            req.timeoutFuture.cancel(false)
          } catch {
            case ignored: Exception => ignored.printStackTrace()
          }

          val p = req.p.asInstanceOf[Promise[Any]]
          val pl = pp.fetchPayload(r)

          pl match {
            case f: RFail => p.tryFailure(RemoteException(f))
            case x => p.tryComplete(Success(x))
          }

        }
    }
  }

  val flushQueue = new Runnable {
    override def run(): Unit = {
      scheduledWrite.set(false)
      ctx.get.flush()
    }
  }

  override def sendReq[X](msg: NodeRequest): Future[X] = {
    val p = strd.concurrent.Promise[X]()

    if (shutdown.get()) {
      p.tryFailure(new RuntimeException("Connector stopped"))
    } else {
      val deadline = if (msg.hasReqDeadline) msg.getReqDeadline else DEFAULT_TIMEOUT

      val time = CTime.now
      val maxTimeInSubmit = ((deadline - time) * 0.2d).toInt

      if (maxTimeInSubmit < 1) {
        p.tryComplete(Failure(new NodeTimeoutException(meta, "req deadline before submit: " + msg.getClass.getName + " time:" + maxTimeInSubmit)))
      } else {
        sendImpl(p, msg, deadline)
      }

    }
    p.future
  }

  def sendImpl(req: NodeRequest) {
    ctx.get.write(req).addListener(channelWriteListener)
    flushRequest()
  }

  def sendImpl(promise: Promise[_], req: NodeRequest, deadline : Long) {
    try {
      val reqId = req.getReqId
      val time = CTime.now

      val delay = deadline - time
      if (delay > 1) {

        // we can do request -> send to node
        val timeoutFutures = dispatcher.timeoutScheduler.schedule(new Runnable() {
          override def run() = {
            reqMap.remove(reqId)

            Stats.incr(reqTimeoutMetricName)
            promise.tryComplete(Failure(new NodeTimeoutException(meta, "req deadline (no reply) timeout: " + delay)))
          }
        }, delay, TimeUnit.MILLISECONDS)

        reqMap.put(reqId, RequestMade(reqId, deadline, promise, timeoutFutures))

        ctx.get.write(req).addListener(channelWriteListener)
        flushRequest()
      } else {
        promise.tryComplete(Failure(new NodeTimeoutException(meta, "req deadline before write: " + req.getClass.getName)))
      }
    } catch {
      case x: Exception => log.error("Failed", x)
    }
  }

  def flushRequest() {
    if (scheduledWrite.compareAndSet(false, true)) {
      ctx.get.executor().execute(flushQueue)
    }
  }

  override def send[X](msg: Message, timeout: Long = 3000, traceRequest: Option[RequestTrace]) = {
    //val trace = traceRequest.getOrElse{ PoolContext.submitToRemote(msg, timeout) }
    val nr = pp.createRequest(msg).asInstanceOf[NodeRequest.Builder]
      .setReqDeadline(CTime.now + timeout)
      .setReqId(reqCounter.incrementAndGet())

    traceRequest.foreach(nr.setTrace)

    if (!traceRequest.isDefined) {
      RemoteTraceContext.remoteRequest(nr, msg, timeout, meta)
    }

    sendReq[X](nr.build())
  }

  override def sendWithoutReply(msg: Message) = {
    val nr = pp.createRequest(msg).asInstanceOf[NodeRequest.Builder]
      .setReqDeadline(CTime.now + DEFAULT_TIMEOUT)
      .setReqId(reqCounter.incrementAndGet())

    RemoteTraceContext.remoteRequest(nr, msg, DEFAULT_TIMEOUT, meta)

    sendImpl( nr.build() )

/*
    if (reqBuffer.offer(RequestSubmitted(nr, None), 5, TimeUnit.SECONDS)) {
      flushRequest()
    } else {
      log.error("req deadline while submitting: " + msg.getClass.getName + " time: 5s")
    }
*/
  }
}
