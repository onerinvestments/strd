package strd


import scala.concurrent.Future
import scala.util.matching.Regex

/**
 *
 * User: light
 * Date: 08/11/13
 * Time: 15:08
 */
package object util {

  implicit def anyToOption[X](any:X) : Option[X] = Some[X](any)
  implicit def paramToValue[X](param : ConfigParam[X]) : X = param.currentValue
  implicit def futureToWithTimeout[X]( f : Future[X]) : FutureWithTimeout[X] = new FutureWithTimeout( f )

  implicit class RegexContext(sc: StringContext) {
    def r = new Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def wrappedException[X]( f: => Future[X]) : Future[X] = {
    try {
      f
    } catch {
      case x:Exception =>
        Future.failed(x)
    }
  }

  implicit val currencyFmt = EnumUtils.intEnumFormat(Currency)
//  implicit def asScala = collection.convert.decorateAll
}
