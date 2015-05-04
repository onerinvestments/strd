package strd.util

import java.nio.ByteBuffer

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 7/29/13
 * Time: 7:02 PM
 */
object StrdSerializer {

  val schema = new SerialSchema
  val serializer = new DynamicSerializer(schema)

  def write[T](o: T) = {
    val clazz = o.getClass

    val dbb = ByteBuffer.allocate(1024)
    val s = schema.serializer(clazz)

    s.write(dbb, o)
    dbb.array()
  }

  def read[T](bytes: Array[Byte])(implicit m: Manifest[T]) = {
    val dbb = ByteBuffer.wrap(bytes)
    val s = schema.serializer(m.runtimeClass)
    s.read(dbb).asInstanceOf[T]
  }

}


object Test extends App {

  StrdSerializer.schema.analyzeObject(Tst2(1, 0L until 5L))

  val w = StrdSerializer.write(Tst(10, "dddd", Some(76)))
  println(StrdSerializer.read[Tst](w))

  val ser = new DynamicSerializer(StrdSerializer.schema)

  val dbb = ByteBuffer.allocate(1024 * 1024 * 10)
  val tst2 = Tst3(7, 0 until 5 map(i => Tst(i, "ddd" + i, Some(i))) toArray)

  println(tst2)
  ser.write(dbb, tst2)
  dbb.position(0)
  val readed = ser.read(ByteBuffer.wrap(dbb.array())).asInstanceOf[Tst3]
  readed.list.foreach(x => print(s"$x "))
  println()
  println(tst2)
  println(readed)
  //assert(tst2.equals(readed))
  println(readed)


}

/*object Test2 extends App {

  var q : Array[Int]= Array(1, 3, 4, 5, 6, -1)
  Tst3(5, q)
  val x = Tst3(1, q)
  val field = x.getClass.getDeclaredField("list")
  val field1 = x.getClass.getDeclaredField("id")
  val offset = JUnsafe.unsafe.objectFieldOffset(field)
  val offset1 = JUnsafe.unsafe.objectFieldOffset(field1)

  val typ = field.getType
  println(typ.getName + " " + field.getType.isArray)
  val base = JUnsafe.unsafe.arrayBaseOffset(typ)
  val index = JUnsafe.unsafe.arrayIndexScale(typ)
  println(JUnsafe.unsafe.arrayBaseOffset(typ))
  println(JUnsafe.unsafe.arrayIndexScale(typ))
  println("-------------")
  println(offset)
  //println(JUnsafe.unsafe.get)
  println(JUnsafe.unsafe.getLong(x, offset))
  println(JUnsafe.unsafe.getInt(x, offset))
  println(JUnsafe.unsafe.getInt(x, offset + 4))
  println(JUnsafe.unsafe.getInt(x, offset + 8))
  println(JUnsafe.unsafe.getInt(x, offset + 12))

  println("---------")
  println(JUnsafe.unsafe.getInt(x, offset + base))
  println(JUnsafe.unsafe.getInt(x, offset + base + 4))
  println(JUnsafe.unsafe.getLong(x, offset + base))
  println("_____________")
  println(JUnsafe.unsafe.getInt(x, offset + base + 8 + index * 4))
  println(JUnsafe.unsafe.getInt(x, offset1))

  println("add size " + JUnsafe.unsafe.addressSize())

  val arr = JUnsafe.unsafe.getObject(x, offset)
  println(arr)
  println(JUnsafe.unsafe.getInt(arr, base + index * 4L))
  println(JUnsafe.unsafe.getInt(arr, base - 4L))
  //println(JUnsafe.unsafe.getAddress(arrAdr))

  val bytes = new Array[Byte](index * 6)
  JUnsafe.unsafe.copyMemory(arr, base, bytes, JUnsafe.unsafe.arrayBaseOffset(bytes.getClass), index * 6)

  val o2 = java.lang.reflect.Array.newInstance(typ.getComponentType, 6)//new Array[Int](6)
  JUnsafe.unsafe.copyMemory(bytes, JUnsafe.unsafe.arrayBaseOffset(bytes.getClass), o2, base, bytes.length)

  println(o2.asInstanceOf[Array[_]].mkString)

  def normalize(value: Int): Long = {
    if (value >= 0) value
    else (~0L >>> 32) & value
  }

}*/

case class Tst(id: Int, name: String, address: Option[Long])

case class Tst2(id: Int, list: Seq[Long])

case class Tst3(id: Int, list: Array[Tst])

