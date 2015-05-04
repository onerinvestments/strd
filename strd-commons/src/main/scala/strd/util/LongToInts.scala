package strd.util

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 23/04/14
 * Time: 18:59
 */
object LongToInts {


  val I_1_MASK = 0x7FFFFFFFL
  val I_2_MASK = 0x7FFFFFFFL << 31
  val EMPTY_MASK = 3L << 62


  def toInts(l: Long): (Int, Int) = {
    if ((l & EMPTY_MASK) != 0) {
      throw new IllegalArgumentException("big value " + l)
    }

    val i1 = (l & I_1_MASK).toInt
    val i2 = ((l & I_2_MASK) >>> 31).toInt


    (i1, i2)
  }

  def toLong(i1: Int, i2: Int) = {
    i1.toLong | (i2.toLong << 31)
  }

  def safeLong(long: Long) = {
    long & ~EMPTY_MASK
  }

}

object LongToIntsTest extends App {
  println(java.lang.Long.toBinaryString(LongToInts.EMPTY_MASK))
  println(java.lang.Long.toBinaryString(LongToInts.I_1_MASK))
  println(java.lang.Long.toBinaryString(LongToInts.I_2_MASK))

  convertAndCheck(0)
  convertAndCheck(1)
  convertAndCheck(4583598345348L)
  convertAndCheck(8844L)
  convertAndCheck(12929292838383833L)
  convertAndCheck(Long.MaxValue / 2)
  convertAndCheck(Long.MaxValue / 2 - 848500034)
  try {
    convertAndCheck(Long.MaxValue)
  } catch {
    case t: Throwable => println("CHECK SUCCESS")
  }
  try {
    convertAndCheck(-1l)
  } catch {
    case t: Throwable => println("CHECK SUCCESS")
  }

  try {
    convertAndCheck(-1595959569l)
  } catch {
    case t: Throwable => println("CHECK SUCCESS")
  }
  try {
    convertAndCheck(Long.MinValue)
  } catch {
    case t: Throwable => println("CHECK SUCCESS")
  }

  def convertAndCheck(l: Long) = {
    val (i1, i2) = LongToInts.toInts(l)
    if(i1 < 0 || i2 < 0) {
      println("ERROR NEGATIME INT")
    }
    val l2 = LongToInts.toLong(i1, i2)
    if (l != l2) {
      println(s"ERROR $l -> $l2 " +
        s"\n\t${java.lang.Long.toBinaryString(l)}" +
        s"\n\t\t${java.lang.Integer.toBinaryString(i1)}" +
        s"\n\t\t${java.lang.Integer.toBinaryString(i2)}" +
        s"\n\t${java.lang.Long.toBinaryString(l2)}")
    } else {
      println(s"SUCCESS $l ${java.lang.Long.toBinaryString(l)}")
    }
  }


}
