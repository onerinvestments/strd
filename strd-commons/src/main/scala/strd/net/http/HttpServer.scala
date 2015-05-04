package strd.net.http

import java.util.concurrent.Executors

import com.twitter.ostrich.admin.Service
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import org.slf4j.LoggerFactory
import strd.trace.PoolContext

import scala.concurrent.Future

/**
 *
 * User: light
 * Date: 17/03/14
 * Time: 13:00
 */

class HttpServer(val dispatcher : () => ChannelHandler,
                 val options: HttpServerOptions = HttpServerOptions()) extends Service {

  val log = LoggerFactory.getLogger(getClass)

  val bossGroup = new NioEventLoopGroup(1)
  val workerGroup = new NioEventLoopGroup(Runtime.getRuntime.availableProcessors() * 2 + 1)

  var ch: Option[Channel] = None

  override def quiesce() = {
    stop()
  }

  def stop() {
    log.info("--> Stopping HttpServer")
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    log.info("-- Stopping (await) HttpServer")
    bossGroup.terminationFuture().sync()
    workerGroup.terminationFuture().sync()

    log.info("<-- Stopped HttpServer")
  }

  override def shutdown() = {

  }

  override def start() = {
    val boot = new ServerBootstrap()
    boot.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new HttpServerChannelInitializer(dispatcher, options))
      .option(ChannelOption.SO_BACKLOG, new Integer(16000))
    //.option(ChannelOption.ALLOCATOR, )
    //.option(ChannelOption.RCVBUF_ALLOCATOR,  AdaptiveRecvByteBufAllocator.DEFAULT)

    ch = Some(boot.bind(options.port).sync().channel())
    log.info(s"HTTP server started with options: $options")
  }
}

object HttpServer {
  def default(handler: RequestHandler, options: HttpServerOptions = HttpServerOptions()) = {
    val scheduler = Executors.newScheduledThreadPool(4)
    val factory = HttpStream.factory(scheduler, options.timeout)

    implicit val execctx = PoolContext.cachedExecutor(name = "http")

    new HttpServer(() => new DefaultChannelHandler(handler, factory), options)
  }
}

class HttpServerChannelInitializer(dispatcher: () => ChannelHandler, options: HttpServerOptions) extends ChannelInitializer[SocketChannel] {
  val allocator = new PooledByteBufAllocator(true)
  val log = LoggerFactory.getLogger(getClass)

  override def initChannel(ch: SocketChannel) = {

    ch.config().setAllocator( allocator )

    val p = ch.pipeline()
    p.addLast("httpDecoder",    new HttpRequestDecoder(options.maxInitialLineLength, options.maxHeaderSize, options.maxChunkSize))
    p.addLast("httpAggregator", new HttpObjectAggregator(options.maxContentLength))
    p.addLast("strdDecoder",    new StrdRequestDecoder())
    p.addLast("httpEncoder",    new HttpResponseEncoder())
    p.addLast("strdEncoder",    new StrdResponseEncoder())
    p.addLast("handler",        dispatcher())
  }
}

case class HttpServerOptions(port                 : Int = 80,
                             maxChunkSize         : Int = 8192,
                             maxInitialLineLength : Int = 4096,
                             maxHeaderSize        : Int = 8192,
                             maxContentLength     : Int = 10240,
                             timeout              : Int = 30000) {

  override def toString = getClass
    .getDeclaredFields.map(_.getName) // all field names
    .zip(productIterator.to)
    .map(x => s"${x._1} = ${x._2}").mkString(", ")
}


object HttpServerTest extends App {
  val log = LoggerFactory.getLogger(getClass)

  val handlerTest = new RequestHandler {
    implicit val execctx = PoolContext.cachedExecutor()

    def handle(req: HttpReq): Future[HttpResp] = Future {
      log.debug(s"Request: $req")

      req.query.getOne("timeout").foreach { t =>
        log.debug(s"sleep $t")
        Thread.sleep(t.toLong)
      }
      HttpResp(
        body = Some(Content("Hello")),
        headers = Map("X-Test" -> Seq("test"))
      )
    }
  }

  HttpServer.default(handlerTest, HttpServerOptions(
    port = 8880
  )).start()
}
