package strd.net

import com.google.protobuf.{ProtoAccessor, ExtensionRegistry, Message}
import com.google.protobuf.GeneratedMessage.{ExtendableMessage, ExtendableBuilder, GeneratedExtension}
import strd.cluster.proto.StrdCluster.{RDone, NodeResponse, NodeRequest}
import strd.cluster.proto.StrdCluster
import strd.dynaschema.ClassWorksHelper


/**
 *
 * User: light
 * Date: 02/04/14
 * Time: 17:00
 */

object StrdProtos {


  val DONE = RDone.newBuilder().build()

  val nodeRequest = NodeRequest.getDescriptor
  val nodeResponse = NodeResponse.getDescriptor

  val bases = Seq(nodeRequest, nodeResponse)

  val definition = new RequestResponseDefinition[NodeRequest, NodeResponse] {
    val respClass = classOf[NodeResponse]
    val reqClass = classOf[NodeRequest]

    override def buildResp = NodeResponse.newBuilder()

    override def buildReq = NodeRequest.newBuilder()
  }

  def build(name : String, cl: Class[_]) =
    new ProtoBuilder()
      .register(classOf[StrdCluster], bases)
      .register(cl, bases)
      .build(name, definition)

  def build(name : String) =
    new ProtoBuilder()
      .register(classOf[StrdCluster], bases)
      .build(name, definition)

  def fetchDescriptorsFromProtoFile( protoClass : Class[_] ) = {
    ClassWorksHelper.fetchDescriptorsFromProtoFile( protoClass )
  }


}

trait RequestResponseDefinition[REQ <: ExtendableMessage[REQ], RESP <: ExtendableMessage[RESP]] {
  def buildReq  : ExtendableBuilder[REQ, _ <: ExtendableBuilder[REQ,_]]
  def buildResp : ExtendableBuilder[RESP,_ <: ExtendableBuilder[RESP,_]]
  def reqClass  : Class[REQ]
  def respClass : Class[RESP]
}

class StrdProto[REQ <: ExtendableMessage[REQ], RESP <: ExtendableMessage[RESP]](
                val name    : String,
                clMap       : Map[Class[_], ProtoPayload],
                rr          : RequestResponseDefinition[REQ,RESP],
                val extReg  : ExtensionRegistry) {

  def fetchPayload(msg: NodeResponse) = {
    ProtoAccessor.getExtension(msg)
  }

  def createResponse[X <: Message](msg: X) = {
    rr.buildResp.setExtension(extensionDesciptor(rr.respClass, msg), msg)
  }

  def extensionDesciptor[X <: Message, Y <: Message](cl: Class[Y], msg: X) = {
    val descriptor = clMap.getOrElse(msg.getClass, {
      throw new IllegalStateException("bad msg :" + msg.getClass.getName)
    })

    descriptor.extField.asInstanceOf[GeneratedExtension[Y, X]]
  }


  def createRequest[X <: Message](msg: X) = {
    rr.buildReq.setExtension(extensionDesciptor(rr.reqClass, msg), msg)
  }

}
