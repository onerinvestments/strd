package strd.tcp

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.chill.ScalaKryoInstantiator
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicReference, AtomicLong}
import com.twitter.ostrich.stats.Stats
import lmbrd.io.JVMReservedMemory
import strd.cluster.{ClusterResponse, ClusterRequest, BinarySizeInfo}
import java.net.InetSocketAddress

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 7/29/13
 * Time: 3:15 PM
 */
object TcpPipeline {

  Stats.addGauge("byteBuffers"){
    JVMReservedMemory.openedBuffers.get()
  }

  def build(handler: ChannelHandler, name : String, maxPacketSize: Long = 1L * 1024 * 1024 * 1024) = {
    new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("defrag",
          new FrameDecoder() {
            def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer) = {

              if (buf.readableBytes() < 4) {
                //println("buf.readableBytes() < 4")
                null
              } else {
                val bodyLength = buf.getInt(buf.readerIndex())
                if ((bodyLength <= 0) || (bodyLength > maxPacketSize)) {
                  throw new AssertionError("bad packet length: " + bodyLength)
                }

                if (buf.readableBytes() >= bodyLength) {
                  buf.readInt() // suppress size
                  buf.readBytes(bodyLength - 4)
                } else {
                  //println(s"buf.readableBytes() < bodyLength ${buf.readableBytes()} $bodyLength" )
                  null
                }
              }
            }
          })

        pipeline.addLast("decoder", new ChannelUpstreamHandler() {
          def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {

//            println(s"receive $e")

            e match {
              case msgEvt: MessageEvent =>

                val start = System.currentTimeMillis()

                val cb = msgEvt.getMessage.asInstanceOf[ChannelBuffer]

                if (cb == null || cb.capacity() == 0) {
                  throw new IllegalStateException("cb = " + cb)
                }

                val arr = cb.array()
                //println(s"dbb arr receive \t${arr.mkString} " )

                val command = ScalaKryoInstantiator.defaultPool.fromBytes(arr)
                val size = arr.length

                command match {
                  case x:BinarySizeInfo => x.bytesSize = size
                  case _ =>
                }

                val toHost = try { ctx.getChannel.getRemoteAddress.asInstanceOf[InetSocketAddress].getHostName } catch {case x:Exception => "" }

                val metricPrefix =
                  toHost + "/"+ (
                    if (size < 1024)   {
                  "<1kb"
                } else if (size < 1024 * 100) {
                  "<100kb"
                } else if (size < 1024 * 500) {
                  "<500kb"
                } else if (size < 1024 * 1024) {
                  "<1mb"
                } else if (size < 1024 * 1024  * 5) {
                  "<5mb"
                } else if (size < 1024 * 1024  * 10) {
                  "<10mb"
                } else  {
                  ">10mb"
                } )

                command match {
                  case x: ClusterRequest =>
//                    Stats.addMetric("network/request_time/" + metricPrefix, Math.max( (System.currentTimeMillis() - x.sentTime).toInt, 0) )
                    x.sentTime = System.currentTimeMillis()
                  case x: ClusterResponse =>
//                    Stats.addMetric("network/response_time" + metricPrefix, Math.max( (System.currentTimeMillis() - x.sentTime).toInt, 0) )
                    x.sentTime = System.currentTimeMillis()
                  case _ =>
                }

//                Stats.addMetric("kryo/decode/" + command.getClass.getSimpleName, System.currentTimeMillis() - start toInt)

                ctx.sendUpstream(new UpstreamMessageEvent(msgEvt.getChannel, command, msgEvt.getRemoteAddress))
              case _ =>
                ctx.sendUpstream(e)
            }
          }
        })

        pipeline.addLast("encoder", new ChannelDownstreamHandler {
          def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
            e match {
              case msgEvt: MessageEvent =>

                val start = System.currentTimeMillis()

                val payload = msgEvt.getMessage
                payload match {
                   case x: ClusterRequest =>
                     x.sentTime = start
//                     Stats.addMetric("packQueue/request/", (x.sentTime - x.creationTime) toInt )

                   case x: ClusterResponse =>
                     x.sentTime = start
//                     Stats.addMetric("packQueue/response/", (x.sentTime - x.creationTime) toInt )

                   case _ =>
                 }

                val bytes = ScalaKryoInstantiator.defaultPool.toBytesWithClass(payload)

                val cb =  ChannelBuffers.buffer(bytes.length + 4)

                cb.writeInt( bytes.length + 4) // for size
                cb.writeBytes(bytes)

//                Stats.addMetric("kryo/encode/" + payload.getClass.getSimpleName, System.currentTimeMillis() - start toInt)


                Channels.write(ctx, e.getFuture, cb)
              case _ =>
                ctx.sendDownstream(e)
            }
          }
        })

        pipeline.addLast("metrics", new MetricsTcpHandler(name) )

        pipeline.addLast("handler", handler)

        pipeline
      }
    }
  }
}

class MetricsTcpHandler(name : String) extends ChannelUpstreamHandler with ChannelDownstreamHandler{

  val rpackags = new ConcurrentHashMap[String, AtomicLong]()
  val wpackags = new ConcurrentHashMap[String, AtomicLong]()

  def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) = {
    e match {
      case m:MessageEvent =>

        val key = m.getMessage.getClass.getName

        var current = wpackags.get( key )

        if (current == null) {
          synchronized {
            current = wpackags.get( key )
            if (current == null) {
              current = new AtomicLong()
              wpackags.put(key, current)

              Stats.addGauge("msg_snt/" +name +"/" + key){
                current.get()
              }
            }
          }
        }
        current.incrementAndGet()

      case _ =>
    }

    ctx.sendDownstream(e)
  }

  def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    e match {
      case m:MessageEvent =>

        val key = m.getMessage.getClass.getName

        var current = rpackags.get( key )

        if (current == null) {
          synchronized {
            current = rpackags.get( key )
            if (current == null) {
              current = new AtomicLong()
              rpackags.put(key, current)

              Stats.addGauge("msg_rcvd/" +name +"/" + key){
                current.get()
              }
            }
          }
        }
        current.incrementAndGet()

      case _ =>
    }

    ctx.sendUpstream(e)
  }
}
