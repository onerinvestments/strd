package strd

import scala.language.experimental.macros

/**
 *
 */
package object bytes {
  type BIN = Array[Byte]
//  implicit class Tuple2Bytes[T1, T2](t: (T1, T2)) {
//    def toBytes: Array[Byte] = macro TupleBytesMacro.toBytesImplicit
//  }
//  implicit class Tuple3Bytes[T1, T2, T3](t: (T1, T2, T3)) { def toBytes = TupleBytes.toBytes(t) }
//  implicit class Tuple4Bytes[T1, T2, T3, T4](t: (T1, T2, T3, T4)) { def toBytes = TupleBytes.toBytes(t) }
//  implicit class Tuple5Bytes[T1, T2, T3, T4, T5](t: (T1, T2, T3, T4, T5)) { def toBytes = TupleBytes.toBytes(t) }
//  implicit class Tuple6Bytes[T1, T2, T3, T4, T5, T6](t: (T1, T2, T3, T4, T5, T6)) { def toBytes = TupleBytes.toBytes(t) }
}
