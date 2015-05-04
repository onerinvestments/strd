package strd.tcp

import org.jboss.netty.channel._
import org.slf4j.LoggerFactory
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ServerBootstrap
import scala.Some
import java.net.InetSocketAddress
import com.twitter.ostrich.admin.Service
import strd.util.IntegerParam


/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 4/11/13
 * Time: 5:31 PM
 */
class TcpServer(val port : Int,
                val pipelineFactory : ChannelPipelineFactory) extends Service {

  val log = LoggerFactory.getLogger(getClass)

  var channel : Option[Channel] = None

  val MAX_PACKET_SIZE =  512 * 1024 * 1024; //512 Kb

  val threads = Runtime.getRuntime.availableProcessors / 2  + 1

  val factory: NioServerSocketChannelFactory = new NioServerSocketChannelFactory(
    Executors.newFixedThreadPool(2),
    Executors.newFixedThreadPool(threads), threads )

  val bootstrap = new ServerBootstrap( factory)


  bootstrap.setPipelineFactory( pipelineFactory )
//  bootstrap.setOption( "child.tcpNoDelay", true )
//  bootstrap.setOption( "child.keepAlive", true )
  bootstrap.setOption( "child.connectTimeoutMillis", 6000 )
//  bootstrap.setOption( "reuseAddress", true )
  bootstrap.setOption( "backlog", 64000 )

  def stop() {
    channel.foreach(ch =>{
      log.debug("Server stopping...")

      ch.close()
      ch.unbind()
      factory.releaseExternalResources()

      log.info("Server stopped")
    })
  }

  def start() {
    channel = Some(  bootstrap.bind( new InetSocketAddress(port) ) )
    log.info("Server Started: " + port)
  }

  override def quiesce() {
    log.debug(s"\t--\tquiesce TcpServer on port $port")
  }

  def shutdown() {
    log.debug(s"\t--\tstop TcpServer on port $port")
    stop()
  }

}
