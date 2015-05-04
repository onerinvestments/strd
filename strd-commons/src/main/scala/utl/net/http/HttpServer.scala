package utl.net.http

import com.twitter.ostrich.admin.Service
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import org.slf4j.LoggerFactory


/**
 *
 * User: light
 * Date: 17/03/14
 * Time: 13:00
 */

class HttpServer(val port: Int, val dispatcher : () => ChannelHandler) extends Service {
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
    log.info("-- HttpServer Stopping (await)")
    bossGroup.terminationFuture().sync()
    workerGroup.terminationFuture().sync()

    log.info("<-- HttpServer Stopped")
  }

  override def shutdown() = {

  }

  override def start() = {
     val boot = new ServerBootstrap()
    boot.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new HttpServerChannelInitializer(dispatcher))
      .option(ChannelOption.SO_BACKLOG, new Integer(16000))
      //.option(ChannelOption.ALLOCATOR, )
      //.option(ChannelOption.RCVBUF_ALLOCATOR,  AdaptiveRecvByteBufAllocator.DEFAULT)

    ch = Some(boot.bind(port).sync().channel())
    log.info("Server started, port: " + port)
  }
}

class HttpServerChannelInitializer(x : () => ChannelHandler) extends ChannelInitializer[SocketChannel] {
  val allocator = new PooledByteBufAllocator(true)

  override def initChannel(ch: SocketChannel) = {

    ch.config().setAllocator( allocator )

    val p = ch.pipeline()
    p.addLast("decoder",  new HttpRequestDecoder())
    p.addLast("combiner", new HttpCombiner() )
    p.addLast("encoder",  new HttpResponseEncoder())
    p.addLast("handler", x())
  }
}
