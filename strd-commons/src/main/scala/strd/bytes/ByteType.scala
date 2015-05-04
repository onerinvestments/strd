package strd.bytes

import scala.reflect.macros.Context
//import scala.reflect.runtime.universe._
import lmbrd.zn.util.PrimitiveBits

object ByteType {
  def byteLength(c: Context)(tpe: c.Type)(v: c.Tree) = {
    import c.universe._

    tpe match {
      case s if s =:= c.weakTypeOf[Short]   => q"${PrimitiveBits.SIZEOF_SHORT}"
      case s if s =:= c.weakTypeOf[Int]     => q"${PrimitiveBits.SIZEOF_INT}"
      case s if s =:= c.weakTypeOf[Long]    => q"${PrimitiveBits.SIZEOF_LONG}"
      case s if s =:= c.weakTypeOf[Float]   => q"${PrimitiveBits.SIZEOF_FLOAT}"
      case s if s =:= c.weakTypeOf[Double]  => q"${PrimitiveBits.SIZEOF_DOUBLE}"
      case s if s =:= c.weakTypeOf[Boolean] => q"${PrimitiveBits.SIZEOF_BOOLEAN}"
      case s if s =:= c.weakTypeOf[String]  => q"PrimitiveBits.toBytes($v: String).size + ${PrimitiveBits.SIZEOF_INT}"
    }
  }

  def putBytes(c: Context)(tpe: c.Type)(b: c.Tree, i: c.Tree, v: c.Tree) = {
    import c.universe._

    tpe match {
      case s if s =:= c.weakTypeOf[Short]   => q"$i = PrimitiveBits.putShort($b, $i, $v)"
      case s if s =:= c.weakTypeOf[Int]     => q"$i = PrimitiveBits.putInt($b, $i, $v)"
      case s if s =:= c.weakTypeOf[Long]    => q"$i = PrimitiveBits.putLong($b, $i, $v)"
      case s if s =:= c.weakTypeOf[Float]   => q"$i = PrimitiveBits.putFloat($b, $i, $v)"
      case s if s =:= c.weakTypeOf[Double]  => q"$i = PrimitiveBits.putDouble($b, $i, $v)"
      case s if s =:= c.weakTypeOf[Boolean] => q"$i = PrimitiveBits.putBoolean($b, $i, $v)"
      case s if s =:= c.weakTypeOf[String]  => q"""
        {
          val __s = PrimitiveBits.getString($v)
          $i = PrimitiveBits.putInt($b, $i, __s.size)
          $i = PrimitiveBits.putBytes($b, $i, __s)
        }
      """
    }
  }

  def readBytes(c: Context)(tpe: c.Type)(b: c.Tree, i: c.Tree) = {
    import c.universe._
    tpe match {
      case s if s =:= c.weakTypeOf[Short]   => q"{val __v = PrimitiveBits.getShort($b, $i); $i += ${PrimitiveBits.SIZEOF_SHORT}; __v}"
      case s if s =:= c.weakTypeOf[Int]     => q"{val __v = PrimitiveBits.getInt($b, $i); $i += ${PrimitiveBits.SIZEOF_INT}; __v}"
      case s if s =:= c.weakTypeOf[Long]    => q"{val __v = PrimitiveBits.getLong($b, $i); $i += ${PrimitiveBits.SIZEOF_LONG}; __v}"
      case s if s =:= c.weakTypeOf[Float]   => q"{val __v = PrimitiveBits.getFloat($b, $i); $i += ${PrimitiveBits.SIZEOF_FLOAT}; __v}"
      case s if s =:= c.weakTypeOf[Double]  => q"{val __v = PrimitiveBits.getDouble($b, $i); $i += ${PrimitiveBits.SIZEOF_DOUBLE}; __v}"
      case s if s =:= c.weakTypeOf[Boolean] => q"{val __v = PrimitiveBits.getBoolean($b, $i); $i += ${PrimitiveBits.SIZEOF_BOOLEAN}; __v}"
      case s if s =:= c.weakTypeOf[String]  => q"""
        {
          val __size = PrimitiveBits.getInt($b, $i)
          $i += ${PrimitiveBits.SIZEOF_INT}
          val __s = PrimitiveBits.toString($b, $i, __size)
          $i += __size
          __s
        }
      """
    }
  }
}
