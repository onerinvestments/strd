package strd

import org.squeryl._
import org.squeryl.dsl.ast._
import org.squeryl.internals._
import play.api.libs.json._
import strd.api.SchemaBase._

/**
 *
 * User: light
 * Date: 18/10/14
 * Time: 18:12
 */
package object api {

  object EnumJson {

    def enumReads[E <: Enumeration](enum: E): Reads[E#Value] = new Reads[E#Value] {
      def reads(json: JsValue): JsResult[E#Value] = json match {
        case JsString(s) =>
          try {
            JsSuccess(enum.withName(s))
          } catch {
            case _: NoSuchElementException =>
              JsError(s"Unknown enumeration value '$s'")
          }
        case _ => JsError("String value expected")
      }
    }

    implicit def enumWrites[E <: Enumeration]: Writes[E#Value] = new Writes[E#Value] {
      def writes(v: E#Value): JsValue = JsString(v.toString)
    }

    implicit def enumFormat[E <: Enumeration](enum: E): Format[E#Value] = {
      Format(enumReads(enum), enumWrites)
    }
  }

  implicit class TableExtension[T](table: Table[T]) {
    def find[K](k: K)(implicit ev: T <:< SoftDelete with KeyedEntity[K]): Option[T] =
      from(table)(a => where(
        FieldReferenceLinker.createEqualityExpressionWithLastAccessedFieldReferenceAndConstant(a.id, k)
          and (a.deleted === false)
      ) select a).headOption

    def exists[K](k: K)(implicit ev: T <:< SoftDelete with KeyedEntity[K]): Boolean =
      from(table)(a => where(
        FieldReferenceLinker.createEqualityExpressionWithLastAccessedFieldReferenceAndConstant(a.id, k)
          and (a.deleted === false)
      ) select a.id).headOption.exists(_ == k)

    def list(implicit ev: T <:< SoftDelete): Query[T] = from(table)(a => where(a.deleted === false) select a)

    def lookupAll[K](keys: Traversable[K])(implicit ev: T <:< KeyedEntity[K]): Seq[T] =
      from(table)(a => where {
        a.id // Hack to trigger last access. It's much easier to get it through takeLastAccessedFieldReference
        val ref = FieldReferenceLinker.takeLastAccessedFieldReference.map(new SelectElementReference(_, NoOpOutMapper))
        new InclusionOperator(ref.get, new RightHandSideOfIn(new ConstantExpressionNodeList[Any](keys, NoOpOutMapper)))
      } select a).toSeq
  }

  trait SoftDelete {
    var deleted = false
  }
}
