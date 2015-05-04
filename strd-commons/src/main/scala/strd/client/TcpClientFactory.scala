package strd.client

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.Executors
import org.jboss.netty.channel.{Channel, ChannelPipeline}
import org.jboss.netty.channel.socket.SocketChannel
import strd.concurrent.Promise

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 7/30/13
 * Time: 12:57 PM
 */
object TcpClientFactory extends NioClientSocketChannelFactory(
  Executors.newFixedThreadPool(2),
  Executors.newFixedThreadPool(16),
  Runtime.getRuntime.availableProcessors) {

  override def newChannel(pipeline: ChannelPipeline): SocketChannel = {
    val ch = super.newChannel(pipeline)
    ch.setAttachment( Promise[Channel]() )
    ch
  }
}
