package strd.net.http

/**
 * @author Kirill chEbba Chebunin
 */
trait MultiStringMapUtils {
  implicit class MultiStringMapWrapper(map: MultiStringMap) {
    def getOne(key: String) = getLast(key)
    def getOneOrElse(key: String, default: => String) = getLast(key).getOrElse(default)

    def getFirst(key: String) = map.get(key).flatMap(_.headOption)
    def getLast(key: String) = map.get(key).flatMap(_.lastOption)

    /**
     * Add value to key seq
     */
    def +(kv: (String, String)) = map + (kv._1 -> map.getOrElse(kv._1, Seq.empty))

    /**
     * Override key with single value
     */
    def :+(kv: (String, String)) = map + (kv._1 -> Seq(kv._2))

    def requireOne(key: String) = getOneOrElse(key, throw new IllegalArgumentException(s"Map key '$key' is required"))
  }
}

object MultiStringMap{
  import scala.collection.JavaConverters._

  def empty = Map.empty[String, Seq[String]]

  def apply(jMap: java.util.Map[String, java.util.List[String]]): MultiStringMap =
    jMap.asScala.toMap.map(x => x._1 -> x._2.asScala.toSeq)


  def apply(jIt: java.lang.Iterable[java.util.Map.Entry[String, String]]): MultiStringMap =
    jIt.asScala.groupBy(_.getKey).map(x => x._1 -> x._2.map(_.getValue).toSeq)
}
