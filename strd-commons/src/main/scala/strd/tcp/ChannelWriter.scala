package strd.tcp

import org.jboss.netty.channel.Channel
import org.slf4j.LoggerFactory

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/14/13
 * Time: 7:25 PM
 */
object ChannelWriter {

  val log = LoggerFactory.getLogger(getClass)

  def writeToChannel(ch: Channel, msg: AnyRef) {
      try{
        if(ch.isConnected) {
          ch.write(msg)
        } else {
          log.warn("channel is not connected")
        }
      } catch {
        case e: Exception => log.warn("channel error ", e)
      }

    }
}
