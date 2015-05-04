package strd.util

import java.lang.reflect.{Field, Modifier}
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

import lmbrd.zn.util.JUnsafe
import org.apache.commons.lang.StringUtils

import scala.util.matching.Regex

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 7/29/13
 * Time: 6:50 PM
 */
object SMemTest extends App{

  println( None.equals( Option(null)) )

  val s = new SMem
  val t = TestCaseClass( "string", 1, Some( TestClass2("str2", 100)), None)
  val zz = TestCaseClass1("STRINGaaaaaaaaaaa", 100L, 20 ,"Aaaaaaaaa")
  s.write( zz )
  s.write( ( 0, 2L) )
  s.write( Seq( 0, 1,3))
  s.write( Seq( ( 0, 2L), "string", 100500L, zz) )
  s.write( t )
  s.write( t.copy(oo = Some("XXX")) )
  s.write( Some(1) )


}

case class TestCaseClass1( a : String, b : Long, i : Int, ss : String, xx : String = "xxx" )
case class TestCaseClass( a : String, b : Int, c : Option[TestClass2] = None, oo : Option[String]  ) {
  @transient var t : String = ""
}
case class TestClass2(n : String, m : Int)

object LMSerials {
  val obj = classOf[java.lang.Object]

  val PREDEF_INT        = 1
  val PREDEF_INT_OBJ    = 2

  val PREDEF_LONG       = 3
  val PREDEF_LONG_OBJ   = 4

  val PREDEF_STRING     = 5
  val PREDEF_NIL        = 6
  val PREDEF_OPTION     = 7
  val PREDEF_SEQ        = 8
  val PREDEF_ARRAY      = 9

}

object IntegerSerializer extends Serializer {
  def code() = LMSerials.PREDEF_INT

  def read(dbb: ByteBuffer) = dbb.getInt

  def write(dbb: ByteBuffer, obj: Any) {
    dbb.putInt(obj.asInstanceOf[Int])
  }
}

object LongSerializer extends Serializer {
  def code() = LMSerials.PREDEF_LONG

  def read(dbb: ByteBuffer) = dbb.getLong

  def write(dbb: ByteBuffer, obj: Any) {
    dbb.putLong(obj.asInstanceOf[Long])
  }
}

object StringSerializer extends Serializer {

  val code = LMSerials.PREDEF_STRING
  val utf8 = Charset.forName("UTF-8")

  def read(dbb: ByteBuffer) = {
    val  ba = new Array[Byte]( dbb.getInt )
    dbb.get(ba)
    new String( ba, utf8 )
  }

  def write(dbb: ByteBuffer, obj: Any) {
    val bytes = obj.asInstanceOf[String].getBytes(utf8)
    dbb.putInt( bytes.length )
    dbb.put( bytes )
  }
}


abstract class FieldReader(val offset : Long)  {

  def serialize(dbb: ByteBuffer, obj: Any)

  def materialize(dbb: ByteBuffer, obj: Any)
}

class LongFieldAccessor(val field : Field) extends FieldReader( JUnsafe.unsafe.objectFieldOffset( field ) ) {

  def serialize(dbb : ByteBuffer, obj : Any) {
    dbb.putLong( JUnsafe.unsafe.getLong(obj,offset) )
  }

  def materialize(dbb : ByteBuffer, obj : Any) {
    JUnsafe.unsafe.putLong(obj, offset, dbb.getLong)
  }
}

class ArrayFieldAccessor(val field : Field) extends FieldReader( JUnsafe.unsafe.objectFieldOffset( field ) ) {

  val elementType = field.getType.getComponentType
  println(s"elemType ${elementType.getName}")
  val scale = JUnsafe.unsafe.arrayIndexScale(field.getType)
  val base: Long = JUnsafe.unsafe.arrayBaseOffset(field.getType)

  def serialize(dbb : ByteBuffer, obj : Any) {
    val arr = JUnsafe.unsafe.getObject(obj, offset)
    val size = JUnsafe.unsafe.getInt(arr, base - 4)

    dbb.putInt( size )
    val bytes = new Array[Byte](scale * size)
    JUnsafe.unsafe.copyMemory(arr, base, bytes, JUnsafe.unsafe.arrayBaseOffset(bytes.getClass), size * scale)
    dbb.put(bytes)
  }

  def materialize(dbb : ByteBuffer, obj : Any) {
    val size = dbb.getInt
    val bytes = new Array[Byte](size * scale)
    dbb.get(bytes)
    val o2 = java.lang.reflect.Array.newInstance(elementType, size)//new Array[Int](6)
    JUnsafe.unsafe.copyMemory(bytes, JUnsafe.unsafe.arrayBaseOffset(bytes.getClass), o2, base, bytes.length)

    JUnsafe.unsafe.putObject(obj, offset, o2)
  }
}

class SeqSerializer(val serializer : Serializer) extends Serializer {
  val code = LMSerials.PREDEF_SEQ

  import scala.collection.mutable

  def read(dbb: ByteBuffer) = {
    val size = dbb.getInt
    val buf = new mutable.ArraySeq[Any](size)
    var i = 0

    while(i < size) {
      buf(i) = serializer.read(dbb)
      i = i + 1
    }

    buf
  }

  def write(dbb: ByteBuffer, obj: Any) {
    val seq = obj.asInstanceOf[Seq[_]].toArray

    val size = seq.length
    dbb.putInt(size)
    var i = 0

    while(i < size) {
      serializer.write(dbb, seq(i))
      i = i + 1
    }
  }
}

class IntFieldAccessor(val field : Field) extends FieldReader( JUnsafe.unsafe.objectFieldOffset( field ) ) {

  def serialize(dbb : ByteBuffer, obj : Any) {
    dbb.putInt( JUnsafe.unsafe.getInt(obj,offset) )
  }

  def materialize(dbb : ByteBuffer, obj : Any) {
    JUnsafe.unsafe.putInt(obj, offset, dbb.getInt)
  }

}

class DynamicSerializer( val schema : SerialSchema ) extends Serializer {
  def code() = -1

  def read(dbb: ByteBuffer) = {
    val code = dbb.getInt
    val s = schema.codes.getOrElse(code, { throw new IllegalArgumentException("Unknown code:" + code)})
    s.read(dbb)
  }

  def write(dbb: ByteBuffer, obj: Any) {
    println(obj.getClass.getName)
    val s = schema.serializer( obj.getClass )
    dbb.putInt(s.code())
    s.write(dbb, obj)
  }

}

class OptionSerializer(val serializer : Serializer) extends Serializer {
  val code = LMSerials.PREDEF_OPTION


  def read(dbb: ByteBuffer) = {
    if ( dbb.get() == 0 ) {
      None
    } else {
      Some( serializer.read(dbb) )
    }
  }

  def write(dbb: ByteBuffer, obj: Any) {
    if (obj == None) {
      dbb.put(0.toByte)
    } else {
      dbb.put(1.toByte)
      serializer.write(dbb, obj.asInstanceOf[Some[_]].get)
    }
  }
}


class ObjectFieldAccessor( field : Field, val serializer : Serializer) extends FieldReader( JUnsafe.unsafe.objectFieldOffset( field ) ) {

  def materialize(dbb: ByteBuffer, obj: Any) {
    val o = serializer.read(dbb)
    JUnsafe.unsafe.putObject(obj, offset, o)
  }

  def serialize(dbb: ByteBuffer, obj: Any) {
    val o = JUnsafe.unsafe.getObject(obj, offset)
    serializer.write(dbb, o)
  }
}

class StringFieldAccessor(field : Field) extends ObjectFieldAccessor( field, StringSerializer )
class OptionFieldAccessor(field : Field, optSerializer : Serializer) extends ObjectFieldAccessor( field, new OptionSerializer(optSerializer) )

abstract class Serializer {
  def code() : Int

  def write(dbb : ByteBuffer, obj : Any)
  def read(dbb : ByteBuffer) : Any

//  def write(cb : ChannelBuffer, obj : Any)
//  def read(cb : ChannelBuffer) : Any
}

class ClassFieldSerializer(val code : Int, val cl : Class[_], val readers : Array[_ <: FieldReader]) extends Serializer {

  def write(dbb : ByteBuffer, obj : Any) {
    var i = 0
    while (i < readers.length) {
      readers(i).serialize( dbb, obj )
      i = i + 1
    }
  }

  def read(dbb : ByteBuffer) : Any = {
    val obj = JUnsafe.unsafe.allocateInstance(cl)
    var i = 0
    while (i < readers.length) {
      readers(i).materialize( dbb, obj )
      i = i + 1
    }
    obj
  }

}

class SerialSchema {

  val codes   = collection.mutable.Map[Int, Serializer]()
  val classes = collection.mutable.Map[Class[_], Serializer]()

  register( classOf[Int],     IntegerSerializer)
  register( classOf[Long],    LongSerializer)

  register( classOf[java.lang.Integer], IntegerSerializer)
  register( classOf[java.lang.Long], LongSerializer)
  register( classOf[String],  StringSerializer)


  def register( cl : Class[_], s : Serializer) {
    codes += (s.code() -> s)
    classes += (cl -> s)
  }

  val optSignature = """Lscala/Option<L([\w\d/]+)*;>;""".r
  val seqSignature = """Lscala/[\w\d/]+<L([\w\d/]+)*;>;""".r

  def buildObjectAnalyzer(code : Int, cl : Class[_]) : ClassFieldSerializer = {
    println("AnalyzeCLASS: " + cl + " => " + code)

    val fields  = cl.getDeclaredFields.filterNot( f => Modifier.isTransient(f.getModifiers) || Modifier.isStatic(f.getModifiers) ).sortBy( _.getName )
    val readers = fields.map( f=> {

      val fType: Class[_] = f.getType
      try {
        val accessor =
          if ( fType == classOf[Long] || fType == java.lang.Long.TYPE) {
            new LongFieldAccessor( f )
          } else if (fType == classOf[Int] || fType == java.lang.Integer.TYPE) {
            new IntFieldAccessor( f )
          } else if (fType == classOf[String] ) {
            new StringFieldAccessor( f )
          } else if (fType == classOf[Option[_]]) {
            // fType.getTypeParameters()(0).
            // Lscala/Option<Lwz/mserial/TestClass2;>;

            val childClass = getChildClass(f, optSignature)

            println("Option with serial: " + childClass.getName)

            new OptionFieldAccessor( f, serializer(childClass) )
          } else if(fType.isArray && fType.getComponentType.isPrimitive) {
            new ArrayFieldAccessor( f )
          } else if(classOf[Seq[_]].isAssignableFrom(fType)) {
            val childClass = getChildClass(f, seqSignature)
            println("params " + fType.getTypeParameters.mkString(" "))

            println("Seq with serial: " + childClass.getName)
            new ObjectFieldAccessor(f, new SeqSerializer(serializer(childClass)))
          } else {
            println("OBJ:" + fType.getName + " at " + cl.getName + "#" + f.getName)
            new ObjectFieldAccessor( f, serializer(fType) )
          }
        println("accessor: " + f.getName +" " + accessor.offset)
        accessor

      } catch {
        case x : Exception => {
          throw new RuntimeException("Near : " + f.getName + " at " + cl.getName + "#" + f.getName, x)
        }
      }
    })

    val s = new ClassFieldSerializer(code, cl, readers)
    register(cl, s)
    s
  }

  val nextSerialId : AtomicInteger = new AtomicInteger(32)

  def serializer(cl : Class[_]) = {
    classes.getOrElse(cl, {
      if (cl.isInterface || Modifier.isAbstract(cl.getModifiers) || cl == classOf[java.lang.Object]) {
        new DynamicSerializer(this)
      } else {
        buildObjectAnalyzer(nextSerialId.incrementAndGet(), cl)
      }
    })
  }

  def getChildClass(f: Field, sig: Regex) = {

    try {
        val sign = f.getClass.getDeclaredField("signature")
        sign.setAccessible( true )
        val signature = sign.get(f).asInstanceOf[String]

        sig.findFirstIn(signature) match {
          case Some( sig(a@_) ) => Thread.currentThread().getContextClassLoader.loadClass( StringUtils.replace(a,"/",".") )
          case None => classOf[java.lang.Object]
        }
      } catch {
        case e: Exception => classOf[java.lang.Object]
      }
  }


  def analyzeObject(o:AnyRef) {
    serializer(o.getClass)
  }

  analyzeObject(None)
  analyzeObject(Some("string"))

  analyzeObject(Nil)
  analyzeObject(Seq(1,2,3))

  analyzeObject((0,2))
  analyzeObject((0L,"str"))

}

class SMem {

  val schema = new SerialSchema

  println("\n\n==== TEST =====")

  def write[T]( o : AnyRef ) {
    val cl      = o.getClass
    println("Write: " + o.getClass +"  ")

    //cl.getTypeParameters.foreach(x=> println(x.getBounds.foreach(b=>println(b))))

    val dbb = ByteBuffer.allocate(1024)

    val s = schema.serializer(cl)

    s.write(dbb, o)
    val bytes = dbb.position()
    println("SIZE: " + bytes)
    dbb.position( 0 )
    val another = s.read(dbb)
    dbb.position( 0 )
    val another2 = s.read(dbb)
    println("\nsrc:\n" + o +" => \n" + another +" eq:" + o.equals(another) + ":" + another.equals(another2) + "\n")

    if ( ! o.equals(another)) {
      throw new RuntimeException
    }

    if (! another.equals(another2)) {
      throw new RuntimeException
    }

    val begin = System.currentTimeMillis()
    var c = 0

    while ( c < 1000000 ) {
      dbb.position(0)
      s.write(dbb, o)
      dbb.position( 0 )
      s.read(dbb)

      c = c + 1
    }

    println("Completed : " + (System.currentTimeMillis() - begin) + " ms -> " + bytes )

/*
    val anal = ClassRegistry.analyzer( o )
    anal.write( dbb, o )
    dbb.position( 0 )

    val obj = anal.read( dbb )
    println( o + "   " + obj )
*/


    //val cb = CharBuffer.wrap("String")
    //val enc = new Chara
    //dbb.put(cb)



/*
    var i = 0
    val begin = System.currentTimeMillis()
    val anal = ClassRegistry.analyzer(o)

    while (i < 10000000) {
      dbb.position(0)
      anal.write(dbb, o)
      dbb.position(0)
      val obj = anal.read(dbb)
      i = i + 1
    }

    val d = System.currentTimeMillis() - begin
*/

    //println( "Done = " + (d) )
  }

}

