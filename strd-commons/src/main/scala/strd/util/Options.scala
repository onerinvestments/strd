package strd.util

/**
 * @author Kirill chEbba Chebunin
 */
trait Options { this: Product =>
  protected def allowEmpty = false

  if (!allowEmpty && isEmpty) {
    throw new IllegalArgumentException("No non-empty options are found")
  }

  protected def options = productIterator.collect { case x: Option[_] => x }
  protected def optionList = options.toList.flatten
  protected def optionListAs[A] = optionList.map(_.asInstanceOf[A])
  protected def isEmpty = options.toList.flatten.isEmpty
}
