package strd.util

import lmbrd.zn.util.TimeUtil

/**
 *
 * User: light
 * Date: 08/11/13
 * Time: 14:59
 */

object ConfigParams {

}

trait ConfigParam[X] {
  def id : String

  var initial : Option[X] = None
  var current : Option[X] = None

  def currentValue = current.orElse(defaultValue.map(defaultFetcher)).getOrElse{ throw new IllegalStateException(s"property:'$id' is not initialized") }
  def initialValue = initial.orElse(defaultValue.map(defaultFetcher)).getOrElse{ throw new IllegalStateException(s"property:'$id' is not initialized") }

  def value = current.orElse(defaultValue.map(defaultFetcher))


  def defaultFetcher(arg : Any) : X = {

    arg match {
      case x:String => parseAndCheck(x)
      case _ => arg.asInstanceOf[X]
    }
  }


  def defaultValue : Option[Any]

  def update(arg : String) {
    val parsed = parseAndCheck( arg )
    current = Some( parsed )
    if (initial.isEmpty) initial = Some(parsed)
  }

  def parseAndCheck(arg : String) : X

//  def listeners : Seq[ ParamListener[X] ]
}

trait ParamListener[X]  {
  def onValueUpdate(oldV : X, newV : X)
}


case class TimeIntervalParam( id           : String,
                              defaultValue : Option[Any] = None,
                              minimal      : Option[Long] = Some(100L),
                              maximal      : Option[Long] = Some(TimeUtil.aDAY) ) extends ConfigParam[Long] {

  val regexp ="""(\d+)(\w)?""".r

  def parseAndCheck(arg: String) = {

    val parsed =
    arg.toLowerCase.trim match {

      case  regexp(ints,str)  =>
        val value = ints.toInt
        str match {

          case "d"  => TimeUtil.aDAY * value
          case "s"  => TimeUtil.aSECOND * value
          case "m"  => TimeUtil.aMINUTE * value
          case "h"  => TimeUtil.aHOUR * value
          case _   => value
        }
      case _ => throw new RuntimeException( s"$arg is not matched")
    }

    minimal.foreach( min => if (parsed < min) throw new IllegalArgumentException(s"Value: '$arg' less then minimum '$minimal' ms. Property: '$id'") )
    maximal.foreach( max => if (parsed > max) throw new IllegalArgumentException(s"Value: '$arg' greater then maximum '$maximal' ms. Property: '$id'") )

    parsed
  }
}

case class BytesIntervalParam( id           : String,
                               defaultValue : Option[Any] = None,
                               minimal      : Option[Long] = Some(0),
                               maximal      : Option[Long] = None ) extends ConfigParam[Long] {

  val regexp ="""(\d+)(\w*)""".r


  def parseAndCheck(arg: String) = {

    val parsed =
    arg.toLowerCase.trim match {

      case  regexp(ints,str)  =>
        val value = ints.toInt
        str match {
          case "k" | "kb" => 1024L * value
          case "m" | "mb" => 1024L * 1024L * value
          case "g" | "gb" => 1024L * 1024L * 1024L * value
          case _   => value
        }
      case _ => throw new RuntimeException( s"$arg is not matched")
    }

    minimal.foreach( min => if (parsed < min) throw new IllegalArgumentException(s"Value: '$arg' less then minimum '$minimal' bytes. Property: '$id'") )
    maximal.foreach( max => if (parsed > max) throw new IllegalArgumentException(s"Value: '$arg' greater then maximum '$maximal' bytes. Property: '$id'") )

    parsed
  }
}

case class IntegerParam(       id           : String,
                               defaultValue : Option[Any] = None,
                               minimal      : Option[Int] = Some(0),
                               maximal      : Option[Int] = None ) extends ConfigParam[Int] {

  def parseAndCheck(arg: String) = {
    val parsed = arg.toInt

    minimal.foreach( min => if (parsed < min) throw new IllegalArgumentException(s"Value: '$arg' less then minimum '$minimal'. Property: '$id'") )
    maximal.foreach( max => if (parsed > max) throw new IllegalArgumentException(s"Value: '$arg' greater then maximum '$maximal'. Property: '$id'") )

    parsed
  }
}



case class LongParam(          id           : String,
                               defaultValue : Option[Any] = None,
                               minimal      : Option[Long] = Some(0L),
                               maximal      : Option[Long] = None ) extends ConfigParam[Long] {

  def parseAndCheck(arg: String) = {
    val parsed = arg.toLong

    minimal.foreach( min => if (parsed < min) throw new IllegalArgumentException(s"Value: '$arg' less then minimum '$minimal'. Property: '$id'") )
    maximal.foreach( max => if (parsed > max) throw new IllegalArgumentException(s"Value: '$arg' greater then maximum '$maximal'. Property: '$id'") )

    parsed
  }
}

case class StringParam(        id           : String,
                               defaultValue : Option[String] = None) extends ConfigParam[String] {

  def parseAndCheck(arg: String) = arg.trim
}

case class ArrayParam(         id           : String,
                               defaultValue : Option[Seq[String]] = Some(Nil)) extends ConfigParam[Seq[String]] {

  def parseAndCheck(arg: String): Seq[String] = arg.trim match {
    case "" => Nil
    case s => s.split(":").map(_.trim) // TODO: escape for ':'
  }
}

trait ConfigParamsDeclaration {
  def apply[X](x: (this.type ) => ConfigParam[X] )(implicit conf : ClusterConfigs) : X = {
    val param = x(this)
    val strValue = conf.props.get( param.id )
    strValue.foreach(x => param.update(x) )

    param.currentValue
  }

  def param[P <: ConfigParam[_] ](x: (this.type ) => P )(implicit conf : ClusterConfigs) : P = {
    val param = x(this)
    val strValue = conf.props.get( param.id )
    strValue.foreach(x => param.update(x) )
    param
  }
}
