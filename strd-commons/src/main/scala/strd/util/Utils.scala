package strd.util

import java.net.URL

import scala.annotation.tailrec

/**
 *
 * User: light
 * Date: 10/2/13
 * Time: 6:20 PM
 */

object Utils {

  def trimUrl( url : String ) = {
    val url1 = new URL( dropRightIf(url, '/', '#', '?') )
    val q = url1.getQuery
    if (q!= null && ! q.isEmpty) {
      url1.getHost + url1.getPath + "?"+ q
    } else {
      url1.getHost + url1.getPath
    }

  }

  @tailrec
  def dropRightIf( str : String, ch : Char * ) : String = {
    if ( ! str.isEmpty && ch.contains( str.last ) ) {
      dropRightIf( str.dropRight(1), ch : _* )
    } else {
      str
    }
  }

  val LOW_SPACE = 50 * 1024d * 1024d * 1024d
  val GIGABYTE = 1024d * 1024d * 1024d


  def freeSpaceWeight(diskFreeBytes : Double ) : Double = {
    val l = math.max( math.log(diskFreeBytes / GIGABYTE / 100d ), 0.001d)

    l * l
  }
}
