package strd.dht

import scala.concurrent.Future
import lmbrd.zn.util.{ByteBuf, PrimitiveBits, MD5Util, BytesHash}
import strd.CByte
import java.io.{DataOutputStream, OutputStream}

/**
 *
 * User: light
 * Date: 9/30/13
 * Time: 12:59 AM
 */

object DHTEntities {

}


trait ConflictResolver {
  def merge( d1 : DHTRow, d2 : DHTRow ) : DHTRow
}

trait ConflictResolver2 {
  def merge( d1 : DHTRow2, d2 : DHTRow2 ) : DHTRow2
}


trait DHTDispatcher {
  def request( req : DHTRequest, toNode : Int ) : Future[DHTResponse]
  def isAlive( nodeId : Int ) : Boolean
}

sealed trait DHTRequest{

}

sealed trait DHTResponse {

}

object DHTPutSuccess extends DHTResponse
object DHTStaleUpdate extends DHTResponse
//object DHTNoData extends DHTResponse

abstract class DHTErrors(id : Int) {

}

object DHTErrors {


  case object UNEXPECTED extends DHTErrors(100)
}

case class DHTError( code : DHTErrors, reason : Option[String] = None ) extends DHTResponse



case class DHTPutRequest( rows      : Seq[DHTRow],
                          toNode    : Int )  extends DHTRequest {
  override def toString = {
    s"Put[${rows.mkString(":")}]"
  }
}

case class DHTPutRequest2( rows      : Seq[DHTRow2],
                          toNode    : Int )  extends DHTRequest {
  override def toString = {
    s"Put[${rows.mkString(":")}]"
  }
}

case class DHTWriteBackOff( rows      : Seq[DHTRow] )  extends DHTRequest {
  override def toString = {
    s"WriteBack[${rows.mkString(":")}]"
  }

}

case class DHTWriteBackOff2( rows      : Seq[DHTRow2] )  extends DHTRequest {
  override def toString = {
    s"WriteBack[${rows.mkString(":")}]"
  }

}

case class DHTGetRequest( keys       : Seq[ BIN ] )  extends DHTRequest {
  override def toString = {
    s"Get[${keys.map(key=>MD5Util.convertToHex(key)).mkString(":")}]"
  }

}

case class DHTGetRequest2( keys       : Seq[ DHTKey2 ] )  extends DHTRequest {
  override def toString = {
    s"Get[${keys.map(key=>s"(${key.table}, ${MD5Util.convertToHex(key.key)})").mkString(":")}]"
  }

}

case class DHTEntries( entries : Seq[DHTEntry] ) extends DHTResponse


/*
case class VersionnedBytes( d : BIN, ver : Long ) {
  override def equals(obj: scala.Any) = {
    val o = obj.asInstanceOf[VersionnedBytes]
    o.ver == ver && BytesHash.compareTo( d, o.d) == 0
  }

  override def hashCode() = {
    BytesHash.instance.computeHashCode( d )
  }
}
*/

case class DHTReplicaNotEnoughException(msg : String = "", thrw : Throwable = null) extends IllegalStateException(msg, thrw)


case class DHTRow( key : BIN, value : BIN, version : Long = System.currentTimeMillis()) {
  override def equals(obj: scala.Any) = {
    val o = obj.asInstanceOf[DHTRow]

    version == o.version && BytesHash.compareTo( value, o.value) == 0 && BytesHash.compareTo( key, o.key) == 0
  }

  override def hashCode() = {
    BytesHash.instance.computeHashCode( value )
  }

  override def toString = {
    s"Row[${MD5Util.convertToHex(key)} -> ${MD5Util.convertToHex(value)}, $version}]"
  }
}

case class DHTKey2( table: Int, key : BIN ) {

  private lazy val _hashCode = BytesHash.instance.computeHashCode(key)

   override def equals(obj: scala.Any) = {
     val o = obj.asInstanceOf[DHTKey2]
     table == o.table && BytesHash.compareTo(key,   o.key) == 0
   }

   override def hashCode() = _hashCode

   override def toString =s"DHTKey($table, ${MD5Util.convertToHex(key)})"

}

case class DHTRow2( table: Int, key : BIN, value : BIN, version : Long = System.currentTimeMillis()) {
  override def equals(obj: scala.Any) = {
    val o = obj.asInstanceOf[DHTRow2]

    table == o.table && version == o.version && BytesHash.compareTo( value, o.value) == 0 && BytesHash.compareTo( key, o.key) == 0
  }

  override def hashCode() = {
    BytesHash.instance.computeHashCode( value )
  }

  override def toString = {
    s"Row[$table, ${MD5Util.convertToHex(key)} -> ${MD5Util.convertToHex(value)}, $version}]"
  }
}

/*
trait DHTEntry {
//  def toValue: DHTEntryValue

  def version : Long
  def md5FromKey : BIN
  def writeValueAndVersionTo( os : OutputStream )
}
*/

object EMPTYDHT extends DHTEntry( new Array[Byte](0), 0)

case class DHTEntry( data : BIN, version : Long ) {

  def writeValueAndVersionTo( bos : ByteBuf ) {
    bos.write(data)
    bos.writeLong(version)
  }

  override def toString = {
    s"DHTEntry[${MD5Util.convertToHex(data)}, $version}]"
  }

  override def equals(obj: scala.Any) = {
    obj match {
      case x : DHTEntry => version == x.version && BytesHash.compareTo(data, x.data) == 0
      case _ => false
    }
  }

}

/*
class DHTEntryWithKey(  kv            : Array[Byte],
                        split         : Int
                      ) extends CByte( kv, 0, split ) with DHTEntry {

  def md5FromKey = MD5Util.md5Bytes(kv, 0, split)

  override def writeKeyTo(bos : OutputStream) {
    bos.write(kv)
  }

  // method write key -> write all data
  def writeValueAndVersionTo(os: OutputStream) = {
    // NOOP
  }

  lazy val version = PrimitiveBits.getLong( kv, kv.length - 8 )

  override def toString = {
    s"EntryKV[${MD5Util.convertToHex(kv, split, kv.length - split - 8)}, $version]"
  }

  def toValue = DHTEntryValue( PrimitiveBits.slice( kv, split, kv.length - 8 - split), version )
}
*/

/*
object DHTEntryWithKey {
  def fromRow(r : DHTRow ) = {
    val kl = r.key.length
    val vl = r.value.length

    val a = new Array[Byte](kl + vl + 8)

    System.arraycopy(r.key, 0, a, 0, kl)
    System.arraycopy(r.value, 0, a, kl, vl)
    PrimitiveBits.putLong(a, kl+vl,  r.version)

    new DHTEntryWithKey(a, kl)
  }
}
*/

trait KeyValueStorage {
  def get(key: CByte): Option[DHTEntry]

  def put(key: CByte, data: DHTEntry)
}