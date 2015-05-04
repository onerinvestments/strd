package com.google.protobuf

import com.google.protobuf.GeneratedMessage.ExtendableMessage
import com.google.protobuf.Descriptors.FieldDescriptor

/**
 *
 * User: light
 * Date: 10/04/14
 * Time: 17:00
 */

object ProtoAccessor {
  val extBuilder = classOf[ExtendableMessage[_]].getDeclaredField("extensions")
  extBuilder.setAccessible(true)

  val ffs = classOf[FieldSet[_]].getDeclaredField("fields")
  ffs.setAccessible(true)

  def getExtension(req : Message) : Message= {
    val extensions = extBuilder.get( req )
    val fieldSet = extensions.asInstanceOf[FieldSet[FieldDescriptor]]
    val array = ffs.get(fieldSet).asInstanceOf[SmallSortedMap[_,_]]
    val count = array.getNumArrayEntries
    var i = 0
    if (count != 1) {
      throw new IllegalStateException("extensions count :" + count)
    }

    val obj = array.getArrayEntryAt(0)
//    val key = obj.getKey.asInstanceOf[FieldDescriptor]
    obj.getValue.asInstanceOf[Message]
  }
}
