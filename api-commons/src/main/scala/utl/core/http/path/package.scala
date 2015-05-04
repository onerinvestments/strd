package utl.core.http


package object path {

  object Parameters {
    def empty: Parameters = Map.empty
  }
  type Parameters = Map[String, Any]

  sealed trait Segment {
    def string: String

    def parse(s: String): Option[Parameters]

//    override def compare(that: Segment) = string.compareTo(that.string)

    override def toString: String = string

//    override def equals(z: Any):Boolean = {
//      if (!z.isInstanceOf[Path]) {
//        false
//      } else {
//        compare(z.asInstanceOf[Segment]) == 0
//      }
//    }

//    override def hashCode(): Int = string.hashCode
  }

  implicit def stringToSegment(s: String): Segment = StaticSegment(s)
  implicit def stringToPath(s: String): Path = Path() / StaticSegment(s)

  case class StaticSegment(string: String) extends Segment {
    def parse(s: String) = s match {
      case `string` => Some(Parameters.empty)
      case _ => None
    }
  }

  object Path {
    def apply(segments: Segment*): Path = apply(segments.toList)
  }

  case class Path(segments: List[Segment]) {

    def parse(s: String): Option[Parameters] = {
      val pathSegments = s.stripPrefix("/").stripSuffix("/").split("/").filter(!_.isEmpty)
      if (pathSegments.size != segments.size) {
        None
      } else {
        segments.zip(pathSegments).foldLeft[Option[Parameters]](Some(Parameters.empty))((r, s) => r match {
          case None => None
          case Some(p) => s._1.parse(s._2).map(p ++ _)
        })
      }
    }

    override def toString: String = "/" + segments.mkString("/")

    def /(s: Segment) = Path(segments :+ s)

    def +(p: Path) = Path(segments ++ p.segments)

    def base(p: Path) = Path(segments.zip(p.segments).takeWhile(Function.tupled(_ == _)).map(_._1))

    def staticPrefix = Path(segments.takeWhile(_.isInstanceOf[StaticSegment]))

    def parameters = segments.collect {
      case p: PathParameter[_] => p
    }
  }

  abstract class PathParameter[T] extends Segment {
    def name: String
    def string = s"{$name}"

    def parseType(s: String): Option[T]

    def parse(s: String) = parseType(s).map(p => Map(name -> p))
  }

  case class int(name: String) extends PathParameter[Int] {
    override def parseType(s: String) = try {
      Some(s.toInt)
    } catch {
      case _: Exception => None
    }
  }

  case class long(name: String) extends PathParameter[Long] {
    override def parseType(s: String) = try {
      Some(s.toLong)
    } catch {
      case _: Exception => None
    }
  }

  case class string(name: String) extends PathParameter[String] {
    override def parseType(s: String) = Some(s)
  }

  case class enum(name: String, enum: Enumeration) extends PathParameter[Enumeration#Value] {
    override def parseType(s: String) = try {
      Some(enum.withName(s))
    } catch {
      case _: Exception => None
    }
  }
}
