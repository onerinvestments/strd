package strd.util

import java.net.{InetAddress, InetSocketAddress, NetworkInterface}


import com.eaio.util.lang.Hex

import scala.collection.mutable.ArrayBuffer

/**
 *
 * User: lembrd
 * Date: 02/05/15
 * Time: 16:11
 */

object InterfaceUtils {
  val interfaces : Seq[IfaceInfo] = {
    val ab = new ArrayBuffer[IfaceInfo]()
    val enums = NetworkInterface.getNetworkInterfaces
    while (enums.hasMoreElements) {
      val e = enums.nextElement()
      ab += IfaceInfo(e)
    }
    ab.toSeq
  }

  def hexMacForIface(iface : NetworkInterface) : String = {
    Hex.append(new java.lang.StringBuilder(36), iface.getHardwareAddress).toString
  }

  def findByName(name : String ) : Option[IfaceInfo] = {
    interfaces.find(x=>x.getName == name)
  }
  /*

      if (e.isUp && !e.isLoopback && !e.isPointToPoint) {
        val aa = e.getInetAddresses
        while (aa.hasMoreElements) {
          val ae = aa.nextElement()
          if (ae.getHostAddress == localOutgoingIp) {
            optIface = Some()
          }
        }
      }
    }

   */
}

case class IfaceInfo( iface: NetworkInterface) {
  val addresses : Seq[InetAddress] = {
    val ab = new ArrayBuffer[InetAddress]
    val ae = iface.getInetAddresses
    while (ae.hasMoreElements) {
      ab += ae.nextElement()
    }
    ab.toSeq
  }



  def getParent: NetworkInterface = iface.getParent

  def isPointToPoint: Boolean = iface.isPointToPoint

  def isUp: Boolean = iface.isUp

  def getDisplayName: String = iface.getDisplayName

  def getHardwareAddress: Array[Byte] = iface.getHardwareAddress

  def isLoopback: Boolean = iface.isLoopback

  override def toString: String = iface.toString

  def getIndex: Int = iface.getIndex

  def isVirtual: Boolean = iface.isVirtual

  def getName: String = iface.getName

  def getHexMac : String = InterfaceUtils.hexMacForIface(iface)

  def getMTU: Int = iface.getMTU
}
