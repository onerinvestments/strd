package strd.util

import scala.util.Random

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 23/03/15
 * Time: 18:21
 */
object RandomUtils {

  def selectByWeight[T](count: Int, seq: Seq[T], weight: T => Double, stepFilter: Option[(T,T) => Boolean]): Seq[T] = {
    if(count == 0 || seq.isEmpty) Nil
    else if(seq.length == 1) Seq(seq.head)
    else {
      var rnd = Random.nextDouble() * seq.map(weight).sum
      seq.find{
        t =>
          rnd -= weight(t)
          rnd <= 0
      }.map{
        selected => selected +: selectByWeight(count - 1, stepFilter.map(f => seq.filter(t => (selected != t) && f(selected, t))).getOrElse(seq.filterNot(_ == selected)), weight, stepFilter)
      }.getOrElse(Nil)
    }
  }

}
