package strd.bytes

import scala.language.experimental.macros
import scala.reflect.macros.Context

object TupleBytes {
  def toBytes[T1, T2](t: (T1, T2)): Array[Byte] = macro TupleBytesMacro.toBytes2[T1, T2]
  def toBytes[T1, T2, T3](t: (T1, T2, T3)): Array[Byte] = macro TupleBytesMacro.toBytes3[T1, T2, T3]
  def toBytes[T1, T2, T3, T4](t: (T1, T2, T3, T4)): Array[Byte] = macro TupleBytesMacro.toBytes4[T1, T2, T3, T4]
  def toBytes[T1, T2, T3, T4, T5](t: (T1, T2, T3, T4, T5)): Array[Byte] = macro TupleBytesMacro.toBytes5[T1, T2, T3, T4, T5]
  def toBytes[T1, T2, T3, T4, T5, T6](t: (T1, T2, T3, T4, T5, T6)): Array[Byte] = macro TupleBytesMacro.toBytes6[T1, T2, T3, T4, T5, T6]

  def fromBytes[T1, T2](b: Array[Byte]): (T1, T2) = macro TupleBytesMacro.toTuple2[T1, T2]
  def fromBytes[T1, T2, T3](b: Array[Byte]): (T1, T2, T3) = macro TupleBytesMacro.toTuple3[T1, T2, T3]
  def fromBytes[T1, T2, T3, T4](b: Array[Byte]): (T1, T2, T3, T4) = macro TupleBytesMacro.toTuple4[T1, T2, T3, T4]
  def fromBytes[T1, T2, T3, T4, T5](b: Array[Byte]): (T1, T2, T3, T4, T5) = macro TupleBytesMacro.toTuple5[T1, T2, T3, T4, T5]
  def fromBytes[T1, T2, T3, T4, T5, T6](b: Array[Byte]): (T1, T2, T3, T4, T5, T6) = macro TupleBytesMacro.toTuple6[T1, T2, T3, T4, T5, T6]
}

object TupleBytesMacro {
  def toBytes(c: Context)(tuple: c.Tree, types: Seq[c.Type]) = {
    import c.universe._

    val typeDefs = types.zipWithIndex.map { t =>
      val method = newTermName(s"_${t._2 + 1}")
      t._1 -> q"t.$method"
    }

    val length = typeDefs.foldLeft(q"0": Tree)((s, i) => q"$s + ${ByteType.byteLength(c)(i._1)(i._2)}")
    val putBytes = typeDefs.map(x => q"${ByteType.putBytes(c)(x._1)(q"bytes", q"idx", x._2)}")

    val code = q"""
      {
        import lmbrd.zn.util.PrimitiveBits

        val t = $tuple

        val length = $length

        val bytes = new Array[Byte](length)
        var idx = 0

        ..$putBytes

        bytes
      }
    """

    c.Expr[Array[Byte]](code)
  }

  def toTuple(c: Context)(bytes: c.Tree, types: Seq[c.Type]) = {
    import c.universe._

    val readBytes = types.map(t => ByteType.readBytes(c)(t)(bytes, q"idx"))

    q"""
      {
        import lmbrd.zn.util.PrimitiveBits

        var idx = 0

        (..$readBytes)
      }
    """
  }

  def toBytes2[T1: c.WeakTypeTag, T2: c.WeakTypeTag](c: Context)
             (t: c.Expr[(T1, T2)]): c.Expr[Array[Byte]] = {
    toBytes(c)(t.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2]))
  }
  def toBytes3[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag](c: Context)
             (t: c.Expr[(T1, T2, T3)]): c.Expr[Array[Byte]] = {
    toBytes(c)(t.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3]))
  }
  def toBytes4[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag](c: Context)
             (t: c.Expr[(T1, T2, T3, T4)]): c.Expr[Array[Byte]] = {
    toBytes(c)(t.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3], c.weakTypeOf[T4]))
  }
  def toBytes5[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag, T5: c.WeakTypeTag](c: Context)
             (t: c.Expr[(T1, T2, T3, T4, T5)]): c.Expr[Array[Byte]] = {
    toBytes(c)(t.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3], c.weakTypeOf[T4], c.weakTypeOf[T5]))
  }
  def toBytes6[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag, T5: c.WeakTypeTag, T6: c.WeakTypeTag](c: Context)
             (t: c.Expr[(T1, T2, T3, T4, T5, T6)]): c.Expr[Array[Byte]] = {
    toBytes(c)(t.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3], c.weakTypeOf[T4], c.weakTypeOf[T5], c.weakTypeOf[T6]))
  }

  def toTuple2[T1: c.WeakTypeTag, T2: c.WeakTypeTag](c: Context)
              (b: c.Expr[Array[Byte]]): c.Expr[(T1, T2)] = {
    c.Expr[(T1, T2)](toTuple(c)(b.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2])))
  }
  def toTuple3[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag](c: Context)
              (b: c.Expr[Array[Byte]]): c.Expr[(T1, T2, T3)] = {
    c.Expr[(T1, T2, T3)](toTuple(c)(b.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3])))
  }
  def toTuple4[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag](c: Context)
              (b: c.Expr[Array[Byte]]): c.Expr[(T1, T2, T3, T4)] = {
    c.Expr[(T1, T2, T3, T4)](toTuple(c)(b.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3], c.weakTypeOf[T4])))
  }
  def toTuple5[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag, T5: c.WeakTypeTag](c: Context)
              (b: c.Expr[Array[Byte]]): c.Expr[(T1, T2, T3, T4, T5)] = {
    c.Expr[(T1, T2, T3, T4, T5)](toTuple(c)(b.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3], c.weakTypeOf[T4], c.weakTypeOf[T5])))
  }
  def toTuple6[T1: c.WeakTypeTag, T2: c.WeakTypeTag, T3: c.WeakTypeTag, T4: c.WeakTypeTag, T5: c.WeakTypeTag, T6: c.WeakTypeTag](c: Context)
              (b: c.Expr[Array[Byte]]): c.Expr[(T1, T2, T3, T4, T5, T6)] = {
    c.Expr[(T1, T2, T3, T4, T5, T6)](toTuple(c)(b.tree, Seq(c.weakTypeOf[T1], c.weakTypeOf[T2], c.weakTypeOf[T3], c.weakTypeOf[T4], c.weakTypeOf[T5], c.weakTypeOf[T6])))
  }

}
