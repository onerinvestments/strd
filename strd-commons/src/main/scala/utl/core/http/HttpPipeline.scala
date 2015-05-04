package utl.core.http

import org.jboss.netty.channel.{ChannelHandler, ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{HttpChunkAggregator, HttpRequestDecoder, HttpResponseEncoder}

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 5/7/13
 * Time: 4:16 PM
 */
object HttpPipeline {


  def build(handler : ChannelHandler, maxPacketSize : Int = 512 * 1024 * 1024)= {
       new ChannelPipelineFactory() {
         def getPipeline = {

           val pipeline = Channels.pipeline()

         		pipeline.addLast( "decoder", new HttpRequestDecoder( 1024 * 600, 1024 * 600, 1024 * 1024 ) )
         		pipeline.addLast( "aggregator", new HttpChunkAggregator( 1024 * 1024 ) )
         		pipeline.addLast( "encoder", new HttpResponseEncoder() )

         		pipeline.addLast( "handler", handler )

         		pipeline

         }
       }
  }

}
