package strd.util

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 10/02/15
 * Time: 16:43
 */
object Currency extends Enumeration {
  val RUB = Value(1)
  val USD = Value(2)

  def forId(id: Int) = values.find(_.id == id).getOrElse {
    throw new NoSuchElementException(s"Id: $id")
  }
}
