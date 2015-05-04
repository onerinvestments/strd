package strd.util

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 10/11/14
 * Time: 21:39
 */
object ResolutionUtils {

  val maxValue = 10000


  def compress(width: Int, height: Int): Int =  {

    if (width < 0 || width >= maxValue) throw new IllegalArgumentException(s"bad width $width min 0 max $maxValue")
    if (height < 0 || height >= maxValue) throw new IllegalArgumentException(s"bad height $height min 0 max $maxValue")
    width * maxValue + height
  }

  def decompress(intResolution: Int): (Int, Int) = (intResolution / maxValue, intResolution % maxValue)

}
