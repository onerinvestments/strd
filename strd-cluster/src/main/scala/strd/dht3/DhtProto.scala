package strd.dht3

import strd.dht3.proto.StrdDhtCluster._
import strd.net._
import strd.cluster.proto.StrdCluster
import com.google.protobuf.{Message, ExtensionRegistry}
import strd.cluster.proto.StrdCluster.{ErrorCode, RFail}
import lmbrd.zn.util
import lmbrd.zn.util.{PrimitiveBits, BytesHash}
import strd.dht3.proto.StrdDhtCluster

/**
 *
 * User: light
 * Date: 15/04/14
 * Time: 21:32
 */

object DhtProto {
  val DONE = DhtDone.newBuilder().build()
  val bases = Seq( DhtSchemaRequest.getDescriptor, DhtSchemaResponse.getDescriptor )

  val definition = new RequestResponseDefinition[DhtSchemaRequest, DhtSchemaResponse] {
    val reqClass = classOf[DhtSchemaRequest]
    val respClass = classOf[DhtSchemaResponse]
    override def buildReq = DhtSchemaRequest.newBuilder()
    override def buildResp = DhtSchemaResponse.newBuilder()
  }

  def build(schemaName : String, protoClasses: Class[_]*): DhtProto = {
    val builder = new ProtoBuilder()
      .register(classOf[StrdCluster], StrdProtos.bases)
      .register(classOf[StrdDhtCluster], DhtProto.bases)

    protoClasses.foreach(builder.register(_, DhtProto.bases))


    new DhtProto(
      protoClasses,
      schemaName,
      builder.clMap.toMap,
      definition,
      builder.extensionRegistry)
  }
}

object DhtErrors {

  def tableNotFound(id : Int) = RFail.newBuilder().setErrorCode(ErrorCode.APP_EXCEPTION).setMsg(s"table $id is not found").build()
  def badRequest(msg : String) = RFail.newBuilder().setErrorCode(ErrorCode.APP_EXCEPTION).setMsg(msg).build()

}

class DhtProto( val protoClasses : Seq[Class[_]],
                val schemaName : String,
                clMap: Map[Class[_], ProtoPayload],
                rr : RequestResponseDefinition[DhtSchemaRequest,DhtSchemaResponse],
                extReg: ExtensionRegistry) extends StrdProto[DhtSchemaRequest,DhtSchemaResponse](schemaName, clMap, rr, extReg) {

  def keyHash(a:Array[Byte]) = {
    BytesHash.instance.computeHashCode(a)
  }

  def keyHash(i: Int): Int = {
    util.MurmurHash3.hash(i)
  }

  def keyHash(l: Long): Int = {
    BytesHash.instance.computeHashCode( PrimitiveBits.longToBytes(l) )
  }


  def createDynamicRequest(tableId : Int, keyHash : Int, payload : Message) : DhtDynamicRequest = {
    val sReq = createRequest( payload )
      .build().toByteString
    DhtDynamicRequest.newBuilder().setTableId(tableId)
      .setKeyHash(keyHash)
      .setRequest(sReq).build()
  }
}
