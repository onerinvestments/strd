package strd.bytes

import lmbrd.zn.util.PrimitiveBits

trait Binary[T] {
  def byteEncoder(value: T): (Int, (BIN, Int) => Unit)
  def readBytes(bin: BIN, i: Int): (Int, T)
}

object Binary extends BinaryPrimitives with BinaryTuples {
  def toBytes[T](v: T)(implicit b: Binary[T]) = {
    val (length, encoder) = b.byteEncoder(v)
    val bytes = new Array[Byte](length)
    encoder(bytes, 0)

    bytes
  }
}

trait BinaryPrimitives {
  implicit object IntBinary extends Binary[Int] {
    def byteEncoder(value: Int): (Int, (BIN, Int) => Unit) = {
      PrimitiveBits.SIZEOF_INT -> {(b, i) => PrimitiveBits.putInt(b, i, value); ()}
    }

    def readBytes(bin: BIN, i: Int): (Int, Int) = PrimitiveBits.SIZEOF_INT -> PrimitiveBits.getInt(bin, i)
  }

  implicit object StringBinary extends Binary[String] {
    def byteEncoder(value: String): (Int, (BIN, Int) => Unit) = {
      val bin = PrimitiveBits.toBytes(value)
      if (bin.size > Short.MaxValue) {
        throw new IllegalArgumentException(s"String size ${bin.size} > short max ${Short.MaxValue}")
      }

      bin.size + PrimitiveBits.SIZEOF_SHORT -> {(b, i) => {
        PrimitiveBits.putShort(b, i, bin.size.toShort)
        PrimitiveBits.putBytes(b, i + PrimitiveBits.SIZEOF_SHORT, bin)
        ()
      }}
    }

    def readBytes(bin: BIN, i: Int): (Int, String) = {
      val size = PrimitiveBits.getShort(bin, i)
      size + PrimitiveBits.SIZEOF_SHORT -> PrimitiveBits.toString(bin, i + PrimitiveBits.SIZEOF_SHORT, size)
    }
  }
}

//class SeqEncoder {
//  def encodeSeq(values: Seq[(Any,Binary[Any])]) = {
//    val encoders = values.map(x => x._2.byteEncoder(x._1))
//
//  }
//}

trait BinaryTuples {
  implicit def tuple2Binary[T1, T2](implicit b1: Binary[T1], b2: Binary[T2]) = new Binary[(T1, T2)] {
    def byteEncoder(value: (T1, T2)): (Int, (BIN, Int) => Unit) = {
      val e1 = b1.byteEncoder(value._1)
      val e2 = b2.byteEncoder(value._2)
      e1._1 + e2._1 -> {(b, i) => {
        e1._2(b, i)
        e2._2(b, i + e1._1)
      }}
    }

    def readBytes(bin: BIN, i: Int): (Int, (T1, T2)) = {
      val t1 = b1.readBytes(bin, i)
      val t2 = b2.readBytes(bin, i + t1._1)

      t1._1 + t2._1 -> (t1._2, t2._2)
    }
  }
}

