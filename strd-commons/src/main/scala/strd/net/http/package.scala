package strd.net

/**
 * @author Kirill chEbba Chebunin
 */
package object http extends MultiStringMapUtils
                            with HttpMethodAliases
                            with HeaderUtils {


  type MultiStringMap = Map[String, Seq[String]]
  type MultiString =  (String, Seq[String])

  val EMPTY_GIF = {
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("empty.gif")
    if (stream == null ) {
      throw new RuntimeException("Can not find resource :empty.gif")
    }
    val ba = new Array[Byte](stream.available())
    stream.read(ba)
    ba
  }
}
