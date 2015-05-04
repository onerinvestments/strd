package strd.net

import com.google.protobuf.{Message, ExtensionRegistry, GeneratedMessage}
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.GeneratedMessage.{ExtendableMessage, ExtendableBuilder, GeneratedExtension}
import strd.cluster.proto.StrdCluster

/**
 *
 * User: light
 * Date: 15/04/14
 * Time: 17:42
 */

class ProtoBuilder {
  import scala.collection.JavaConverters._

  import collection.mutable

  val clMap = mutable.Map[Class[_], ProtoPayload]()
  val extensionDescriptor = mutable.Map[String, GeneratedMessage.GeneratedExtension[_, _]]()

  val extensionRegistry = ExtensionRegistry.newInstance()

  def register(protoClass: Class[_], baseClasses: Seq[Descriptor]) = {
    StrdCluster.registerAllExtensions(extensionRegistry)
    protoClass.getMethod("registerAllExtensions", classOf[com.google.protobuf.ExtensionRegistry]).invoke(null, extensionRegistry)

    StrdProtos.fetchDescriptorsFromProtoFile(protoClass).foreach(x => {
//      println("Found extension:" + x.getName)

      val exts = x.getExtensions.asScala

      baseClasses.foreach(base => {
        exts.find(_.getContainingType.equals(base)).foreach(y => {
//          println("Found action class:" + x.getName + " as:" + y.getNumber + " " + x)
          val pl = protoPayload(protoClass, x)
          clMap += (pl.cl -> pl)
        })

      })
    })
    this
  }


  def build[REQ <: ExtendableMessage[REQ], RESP <: ExtendableMessage[RESP]](name :String, definition: RequestResponseDefinition[REQ, RESP]) = {
    new StrdProto[REQ,RESP](name, clMap.toMap, definition, extensionRegistry)
  }


  private def protoPayload(protoClass: Class[_], x: Descriptor) = {
//    println("Analyze protos from thread: " + Thread.currentThread().getName + " withClassLoader:" + Thread.currentThread().getContextClassLoader )

    val cl = Thread.currentThread().getContextClassLoader.loadClass(protoClass.getName + "$" + x.getFullName)

//    println("Analyze:" + x.getFullName + " -> " + cl.getName)

    val m = cl.getDeclaredMethod("getDefaultInstance")

    val genExt = com.google.protobuf.GeneratedMessage.newMessageScopedGeneratedExtension(
      m.invoke(null).asInstanceOf[Message],
      0,
      cl,
      m.invoke(null).asInstanceOf[Message])

    ProtoPayload(cl, x, genExt)
  }
}

case class ProtoPayload(cl: Class[_], d: Descriptor, extField: GeneratedExtension[_ <: Message, _])
