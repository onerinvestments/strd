package strd.util

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{OverlappingFileLockException, FileLock, FileChannel}

/**
 *
 * User: light
 * Date: 31/10/13
 * Time: 15:29
 */

class SequenceFile(f : File) {

  def get = underFile(valueFromChannel)

  def underFile[X](handler :(FileChannel) => X) = {
    val raf = new RandomAccessFile(f, "rws")
    val channel = raf.getChannel

    var lock :FileLock= null
    while (lock == null) {
      try {
        lock = channel.tryLock()
      } catch {
        case x: OverlappingFileLockException =>
      }
    }

    try {
      handler(channel)
    } finally {
      lock.release()
      channel.close()
      raf.close()
    }
  }

  def valueFromChannel(channel : FileChannel) = {
    if (channel.size() == 0) {
      0
    } else {
      val bytes = new Array[Byte](channel.size().toInt)
      val bb = ByteBuffer.wrap(bytes)
      channel.read(bb)
      val str = new String(bytes)
      str.trim.toInt
    }
  }

  def nextId = underFile{ channel =>
    val next = valueFromChannel(channel) + 1
    channel.position(0)
    channel.write( ByteBuffer.wrap( next.toString.getBytes ) )
    next
  }

  def set(next: Int) = underFile {
    channel =>
      channel.position(0)
      channel.write(ByteBuffer.wrap(next.toString.getBytes))
  }

}
