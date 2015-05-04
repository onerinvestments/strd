package strd.util

import play.api.libs.json._

import scala.reflect.Manifest

abstract class Enum[A] extends Enumeration {

  def all = values.map(_.asInstanceOf[A])

  def optId(id: Int) = values.find(_.id == id).map(_.asInstanceOf[A])
  def forId(id: Int) = optId(id).getOrElse {
    throw new NoSuchElementException(s"Id: $id")
  }

  def optName(name: String) = values.find(_.toString == name).map(_.asInstanceOf[A])
  def forName(name: String) = optName(name).getOrElse {
    throw new NoSuchElementException(s"Name: $name")
  }

  def instance[B](t: A, params: String)(implicit ev: A <:< ClassType[B]): B = t.fromJson(params)
  def instance[B](id: Int, params: String)(implicit ev: A <:< ClassType[B]): B = instance(forId(id), params)

  def classType[B](o: B)(implicit ev: A <:< ClassType[_]): A = classType(o.getClass)

  def classType[B](implicit ev: A <:< ClassType[_], m: Manifest[B]): A = classType(m.runtimeClass)

  def classType(cl: Class[_])(implicit ev: A <:< ClassType[_]): A = all.find { v =>
    v.clazz.isAssignableFrom(cl)
  }.get

  abstract class ClassType[B](id: Int, name: String)(implicit val fmt: Format[B], m: Manifest[B]) extends Val(id, name) {
    def this(id: Int)(implicit fmt: Format[B], m: Manifest[B]) = this(id, if (nextName != null && nextName.hasNext) nextName.next() else null)

    val clazz = m.runtimeClass

    def toJson(f: B) = Json.toJson(f)
    def fromJson(s: String)   : B = fromJson(Json.parse(s))
    def fromJson(js: JsValue) : B = Json.fromJson[B](js).get
  }
}


