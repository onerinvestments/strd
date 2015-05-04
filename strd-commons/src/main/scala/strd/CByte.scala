package strd

import lmbrd.zn.util.{MD5Util, PrimitiveBits, BytesHash}
import strd.dht.DHTEntry
import java.io.OutputStream

/**
 *
 * User: light
 * Date: 9/29/13
 * Time: 4:11 AM
 */

/*
trait CByte extends Comparable[CByte] {
  def bytes : Array[Byte]

  def keyOffset : Int
  def keyLength : Int
}
*/

class CByte( val buf   : Array[Byte] ) extends Comparable[CByte]{

//  lazy val len = if (_len == -1 ) buf.length else _len

  private lazy val _hashCode = BytesHash.instance.computeHashCode(buf)

  def compareTo(o: CByte): Int = BytesHash.compareTo(buf,   o.buf)

  def writeKeyTo(bos : OutputStream) {
    bos.write(buf)
  }

  override def equals(obj: scala.Any) = compareTo( obj.asInstanceOf[CByte] ) == 0

  override def hashCode() = _hashCode

  override def toString =s"'${MD5Util.convertToHex(buf)}'"

  def len = buf.length
}

object CByte {
  val EMPTY = new CByte(new Array[Byte](0))

  def apply( ba : Array[Byte]) = new CByte(ba)
//    if (len == -1) new CByte(ba)


}