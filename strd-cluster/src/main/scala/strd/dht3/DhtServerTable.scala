package strd.dht3

import com.google.protobuf.Message
import strd.cluster.proto.StrdCluster.RDone
import strd.dht.BIN
import strd.dht3.proto.StrdDhtCluster._
import strd.net.{RequestContext, StrdProtos}

/**
 *
 * User: light
 * Date: 15/04/14
 * Time: 12:25
 */

trait DhtServerTable {
  def shutdown()


  def tableId : Int

  def handleRequest( req : Message, ctx : SchemaContext )

}

class SchemaContext(val proto      : DhtProto,
                        nodeReqCtx : RequestContext ) {

  def !!(x:Exception) {
    nodeReqCtx !! x
  }

  def !(r : Message) {
    r match {
      case x   : DhtDone =>
        nodeReqCtx ! StrdProtos.DONE
      case x   : RDone =>
        nodeReqCtx ! StrdProtos.DONE

      case msg : Message =>
        try {
          val b     = proto.createResponse(msg).build()
          val bytes = b.toByteString
          val resp  = DhtDynamicResponse.newBuilder().setResponse(bytes).build()

          nodeReqCtx ! resp
        } catch {
          case x: Exception =>
            nodeReqCtx.!!(x)
        }
    }
  }

}

trait DhtStorage3 {
  def shutdown()
  def erase()
  def backup()
}

trait DhtLevelDbStorage extends DhtStorage3 {
  def get(bytes: BIN) : Option[BIN]

  def put( bytes : BIN, value: BIN )
  def scan( from : BIN, to : BIN)( f : (BIN,BIN) => Boolean ) : Unit

  def delete(bytes: BIN)
}

trait DhtDbApi {
  def getStorage[X](storageId : String, workerId : Int)(implicit m: Manifest[X]) : X
  def getStorageOpt[X](storageId : String, workerId : Int)(implicit m: Manifest[X]) : Option[X]
  def deleteStorage(storageId : String, workerId : Int, backup: Boolean = true): Unit
}

trait TableActivator[T <: DhtServerTable] {
  def tableClass: Class[T]

  def parentMetadata: Seq[ServerTableMetadata[_]] = Seq.empty

  def tableMetadata: ServerTableMetadata[T] = {
    ServerTableMetadata[T](tableClass, (tableDependencies ++ parentMetadata.flatMap(_.allClasses)).toSet)
  }

  def initTable(tableId : Int, dbApi : DhtDbApi, workerId : Int) : T

  def tableDependencies = Seq.empty[Class[_]]
}
