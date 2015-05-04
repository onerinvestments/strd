package strd.net

import com.google.protobuf.Message
import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{ByteToMessageDecoder, LengthFieldBasedFrameDecoder, LengthFieldPrepender, MessageToByteEncoder}
import strd.cluster.proto.StrdCluster.{HandshakeRequest, HandshakeResponse, NodeRequest, NodeResponse}


/**
 *
 * User: light
 * Date: 31/03/14
 * Time: 18:19
 */

object NettyProtobufCodec {
  //def serverPipeline( p:Pipeli)
}

class NettyProtobufEncoder(val pp : StrdProto[_,_]) extends MessageToByteEncoder[Message] {

  override def acceptOutboundMessage(msg: scala.Any) = {
    val status = super.acceptOutboundMessage(msg)
/*
    if (! status) {
      throw new IllegalStateException("Bad message type:" + msg)
    }
*/
    status
  }

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: ByteBuf) = {
    val bos = new ByteBufOutputStream( out )

    msg match {
      case x: NodeRequest       => bos.writeByte(1)
      case x: NodeResponse      => bos.writeByte(2)
      case x: HandshakeRequest  => bos.writeByte(3)
      case x: HandshakeResponse => bos.writeByte(4)
    }

    msg.writeTo(bos)
  }

}

class ProtobufChannelInitializer(pp: StrdProto[_,_],
                                 init : () => ChannelHandler,
                                 maxPacketSize: Int = 1 * 1024 * 1024 * 1024) extends ChannelInitializer[SocketChannel] {

//  val allocator = new PooledByteBufAllocator(true)

  override def initChannel(ch: SocketChannel) = {
//    ch.config().setAllocator( allocator )

    val p = ch.pipeline()
    p.addLast( "defrag",  new LengthFieldBasedFrameDecoder( maxPacketSize, 0, 4, 0, 4 ) )
    p.addLast( "lengthPrepender", new LengthFieldPrepender(4))

    p.addLast( "decoder", new NettyProtobufDecoder(pp) )
    p.addLast( "encoder", new NettyProtobufEncoder(pp) )

    p.addLast( "handler", init() )
  }

}



class NettyProtobufDecoder(val pp : StrdProto[_,_]) extends ByteToMessageDecoder{

  override def decode(ctx: ChannelHandlerContext,
                      in : ByteBuf,
                      out: java.util.List[AnyRef]) = {

    if ( in.isInstanceOf[EmptyByteBuf] ) {
      // TODO: Logging
//      println("Channel closed? " + ctx.channel())

    } else {
      val tp = in.readByte()
      val stream = new ByteBufInputStream(in)

      tp match {
        case 1 =>
          out.add( NodeRequest.parseFrom(stream, pp.extReg) )

        case 2 =>
          out.add( NodeResponse.parseFrom(stream, pp.extReg) )

        case 3 =>
          out.add( HandshakeRequest.parseFrom(stream, pp.extReg) )

        case 4 =>
          out.add( HandshakeResponse.parseFrom(stream, pp.extReg) )
      }
    }
  }
}



