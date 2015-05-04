package strd.dht3

import scala.language.experimental.macros
import scala.reflect.macros.Context

object ServerTableMetadata {
  def apply[T](dependencies: Iterable[Class[_]] = Nil)(implicit m: Manifest[T]): ServerTableMetadata[T] = apply(m.runtimeClass.asInstanceOf[Class[T]], dependencies.toSet)

  def parentMetadata[T]: Seq[ServerTableMetadata[_]] = macro ServerTableMetadataMacro.readTableClasses[T]
}
case class ServerTableMetadata[T](baseClass: Class[T], dependencies: Set[Class[_]] = Set.empty) {
  def allClasses = Set(baseClass) ++ dependencies
}

object ServerTableMetadataMacro {
  def readTableClasses[T: c.WeakTypeTag](c: Context): c.Expr[Seq[ServerTableMetadata[_]]] = {
    import c.universe._

    val base = weakTypeOf[T]
    val parents = base.baseClasses.drop(1).map(x => base.baseType(x))

    val metas = parents.flatMap { tpe =>
      val implicitMeta = appliedType(weakTypeOf[ServerTableMetadata[_]].typeConstructor, tpe :: Nil)
      c.inferImplicitValue(implicitMeta) match {
        case EmptyTree => None
        case impl => Some(impl)
      }
    }

    val seq = weakTypeOf[Seq[ServerTableMetadata[_]]].typeSymbol.companionSymbol
    val result = Apply(Select(Ident(seq.name), newTermName("apply")), metas)

    c.Expr[Seq[ServerTableMetadata[_]]](result)
  }
}
