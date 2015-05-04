package strd.util

import java.net.{Socket, InetSocketAddress}
import org.slf4j.LoggerFactory
import java.io.{IOException, EOFException, File}
import lmbrd.zn.util.PrimitiveBits
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.nio.ByteBuffer
import strd.dht.BIN
import java.util.concurrent.TimeUnit
import java.nio.channels.SocketChannel
import com.twitter.ostrich.stats.Stats
import lmbrd.io.JVMReservedMemory

/**
 *
 * User: light
 * Date: 06/02/14
 * Time: 19:16
 */
class PipedInputData( addr : InetSocketAddress, uuid : String,  dir : File ) {


  val socket = new Socket()


  val tmpFile = new File(dir, uuid + ".pipeIn.tmp")
  tmpFile.createNewFile()
  tmpFile.deleteOnExit()

  val log = LoggerFactory.getLogger(getClass.getName + "." + uuid.substring(0, 10))

  val availableBytes  = new AtomicLong(0)

  val lock = new ReentrantLock()
  val cond = lock.newCondition()

  val bb = new MMFByteBuf(tmpFile, PipedTransfer.BUFFER_STEP)
  
  PipedTransfer.pipesIn.incrementAndGet()
  Stats.incr("pipes/in/" + addr.getHostString)
  PipedTransfer.addPipe( uuid )

  val MSG_CONTINUE = ByteBuffer.wrap( PrimitiveBits.intToBytes(1) )
  val MSG_CLOSE = ByteBuffer.wrap( PrimitiveBits.intToBytes(3) )

  val worker = new Thread() {
    val channel = SocketChannel.open()

    def readFragmented( sc : SocketChannel, b : ByteBuffer, countMin : Int) {
      var readedBytes = 0

      while( readedBytes < countMin && !Thread.currentThread().isInterrupted  ) {
        val bytes = sc.read( b )

        if (bytes == -1) {
          throw new EOFException()
        }

        readedBytes += bytes
      }

    }

    override def run() = {
      val buffer = JVMReservedMemory.buffer(PipedTransfer.PACKET_SIZE)
      try {
//        log.debug("CONNECT: " + addr)

        if ( ! channel.connect( addr ) ) {
          throw new RuntimeException("Can not connect")
        }

        MSG_CONTINUE.position(0)
        channel.write( MSG_CONTINUE )

        readFragmented(channel, buffer, 4)
        buffer.flip()

        while( isFinished.get() == 0 && !Thread.currentThread().isInterrupted  ) {
          var packRemaining = buffer.getInt

          if (packRemaining == -1) {
            MSG_CLOSE.position(0)
            channel.write( MSG_CLOSE )
            isFinished.set(1)

          } else {
            // we need to read packLength

            while (packRemaining > 0) {

              val toRead = math.min( packRemaining, buffer.remaining())
              packRemaining -= toRead
              lock.lock()
              try {
                val slice = buffer.slice()
                slice.limit( toRead )

                bb.write(slice)
                availableBytes.addAndGet( toRead )

                bytesReceived += toRead

                cond.signalAll()
              } finally {
                lock.unlock()
              }

              buffer.position( buffer.position() + toRead )

              if ( buffer.remaining() > 0 ) {
                buffer.compact()
              } else {
                buffer.clear()
              }

              if ( packRemaining > 0 ) {
                // should read more
                readFragmented(channel, buffer, packRemaining)
                buffer.flip()
              } else {  // packet fully readed
                MSG_CONTINUE.position(0)
                channel.write( MSG_CONTINUE )

                readFragmented(channel, buffer, 4)
                buffer.flip()
              }
            }
          }
        }
      } catch {
        case x:Exception =>
          log.warn("Exception", x)
          setFailed()
      } finally {
        JVMReservedMemory.cleanBuffer( buffer )
//        log.debug(s"PIPE CLOSE rcvd:$bytesReceived / toRead: ${availableBytes.get()}  ")
        isFinished.compareAndSet(0, 2)

        if ( availableBytes.get() == 0 ) {
          cleanupBuffer()
        }

        try { channel.close() } catch { case x: Exception => }
        try { socket.close() } catch { case x: Exception => }

      }
    }
  }

  worker.start()

  def cleanupBuffer() {
    if (isClosed.compareAndSet(false, true)) {
      PipedTransfer.pipesIn.decrementAndGet()
      PipedTransfer.removePipe( uuid )

      Stats.incr("pipes/in/" + addr.getHostString, -1)

      try { tmpFile.delete() } catch { case x: Exception => }
      bb.close()
    }
  }

  def setFailed() {
    isFinished.set(2)
    lock.lock()
    try {
      cond.signalAll()
    } finally {
      lock.unlock()
    }
  }


  val isFinished = new AtomicInteger()

  var crBuffer : Int = 0
  var crBytes  : Int = 0

  var bytesReceived : Long = 0

  val isClosed = new AtomicBoolean()

  def readBytes(array : BIN, offset : Int, count : Int) = {

    var toRead = count
    var off = offset

    while (toRead > 0) {

      if (bb.getWrittenBytes(bb.getBuffersDirty()(crBuffer)) == crBytes) {
        bb.cleanup(crBuffer)
        // TODO: cleanUP currentBuffer
        crBytes = 0
        crBuffer += 1
      }

      val currentBuffer = bb.getBuffersDirty()(crBuffer)
      val bytesWritten = bb.getWrittenBytes(currentBuffer)
      val bytesRemains = bytesWritten - crBytes

      val toReadFromThisBuffer = math.min(bytesRemains, toRead)

      val pp = currentBuffer.position()
      currentBuffer.position(crBytes)

      val s = currentBuffer.slice()
      s.limit(toReadFromThisBuffer)

      currentBuffer.position(pp)

      s.get(array, off, toReadFromThisBuffer)

      off += toReadFromThisBuffer
      toRead -= toReadFromThisBuffer
      crBytes += toReadFromThisBuffer
    }

    if ( availableBytes.addAndGet( -count) == 0 && isFinished.get() != 0 ) {
      cleanupBuffer()
    }

    array
  }

  def readAvailableBytes(array : BIN, offset : Int, count : Int) = {
    lock.lock()
    try {
      var avail = availableBytes.get()

      while ( avail < count && isFinished.get() == 0 && !Thread.currentThread().isInterrupted) {
        cond.await(100, TimeUnit.MILLISECONDS)
        avail = availableBytes.get()
      }

      if (avail == 0 && isFinished.get()!=0 ) {
        if (isFinished.get() == 1) {
          cleanupBuffer()
          throw new EOFException("Bytes received: " + bytesReceived)
        } else {
          cleanupBuffer()
          throw new IOException("Failed")
        }
      } else if (avail < count) {
        cleanupBuffer()
        throw new IllegalStateException()
      }

      readBytes(array, offset, count )

    } finally {
      lock.unlock()
    }
  }

  val lenBuf = new Array[Byte](4)

  def read() = {
    val e = new PipedEntry()
    readEntry(e)

    e.data
  }

  def readEntry( entry : PipedEntry ) {

    val valueLen = PrimitiveBits.getInt( readAvailableBytes(lenBuf,0, 4), 0 )
    if (entry.data == null || entry.data.length < valueLen) {
      entry.data = new Array[Byte]( valueLen )
    }

    entry.length = valueLen
    readAvailableBytes(entry.data, 0, valueLen)
  }

}
