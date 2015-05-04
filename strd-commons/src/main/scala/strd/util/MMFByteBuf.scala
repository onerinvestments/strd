package strd.util

import java.io.{File, RandomAccessFile}
import lmbrd.zn.util.ByteBuf
import java.nio.channels.FileChannel.MapMode
import java.nio.ByteBuffer
import lmbrd.io.JVMReservedMemory

/**
 *
 * User: light
 * Date: 06/02/14
 * Time: 19:06
 */

class MMFByteBuf(file: File, initial: Int) extends ByteBuf() {
  var lastOffset: Long = 0L
  val bChannel = new RandomAccessFile(file, "rw").getChannel


  protected override def allocate(size: Int) = {
    JVMReservedMemory.openedBuffers.incrementAndGet()
    val channel = bChannel.map(MapMode.READ_WRITE, lastOffset, initial)
    //println("ALLOCATE: " + lastOffset + " " + initial)
    lastOffset += initial

    channel
  }

/*
  protected override def allocate(size: Int) = {
    ByteBuffer.allocate(size)
  }
*/

  override def close() = {
    //println( "CLOSE: " + file.getName )

    (0).until(buffers.length).foreach( x => {
      if ( buffers(x) != null ) {
        buffers(x).clear()
        cleanup(x)
      }
    } )
    try { bChannel.close() } catch { case x: Exception => }
  }

  buffers(0) = allocate(initial)
}
