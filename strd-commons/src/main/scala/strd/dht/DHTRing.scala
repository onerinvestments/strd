package strd.dht

import java.util
import strd.CByte
import lmbrd.zn.util.MD5Util
import java.util.Comparator

/**
 *
 * User: light
 * Date: 9/27/13
 * Time: 1:35 PM
 *
 */


class DHTRing(val tokens: Seq[DHTToken]) {

  if ( tokens.exists(_.node == 0) || tokens.isEmpty ) {
    throw new IllegalStateException("Unassigned tokens found " + tokens)
  }

  val keys    = new Array[ CByte ](tokens.size)
  val values  = new Array[ NodeElem ](tokens.size)

//  val mp = new util.TreeMap[CByte, NodeElem ]()
  tokens.sortBy( x => CByte(MD5Util.fromHex(x.key)) ).zipWithIndex.foreach( t => {
    keys(t._2) = CByte(MD5Util.fromHex(t._1.key))
    values(t._2) = NodeElem( t._1.node, Nil)
  } )

  values.zipWithIndex.foreach( v => {
    val prefs = circular(values.toSeq, v._2).map(_.nodeId).distinct
    values(v._2) = v._1.copy( next = prefs )
  })

  def circular[X]( ar:Seq[X], idx :Int ) = {
    ar.drop(idx) ++ ar.take(idx)
  }

  val cmp = new Comparator[CByte] {
    def compare(o1: CByte, o2: CByte) = o1.compareTo(o2)
  }

  def find( key : CByte) = {
    val r = util.Arrays.binarySearch( keys, key, cmp )
    if (r < 0) {  // insertion point
      val idx = -r -1
      values( if (idx == keys.length ) 0 else idx )
    } else {
      values( r )
    }
  }

}

case class NodeElem( nodeId : Int, next : Seq[Int] )