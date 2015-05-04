package strd

/**
 *
 * User: light
 * Date: 10/2/13
 * Time: 5:05 AM
 */
package object dht {

  type BIN = Array[Byte]


  def numToName(num: Int): String = {
    val s = Integer.toString(num, 16)
    val c : Int = math.max(16 - s.length, 0)
    ("0" * c) + s
  }

  def nameToNum(name: String): Int = Integer.parseInt(name, 16)


  def foreach[K,V](m : java.util.Map[K,V])( f: (K,V) => Unit ) {
    val i = m.entrySet().iterator()

    while (i.hasNext) {
      val e = i.next()
      f(e.getKey, e.getValue)
    }
  }



}
