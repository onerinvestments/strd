package strd.util

import java.net.InetSocketAddress
import org.slf4j.LoggerFactory
import java.io.File
import strd.concurrent.Promise
import lmbrd.zn.util.{PrimitiveBits, UUID}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.nio.{BufferOverflowException, ByteBuffer}
import strd.dht.BIN
import java.util.concurrent.TimeUnit
import java.nio.channels.{SocketChannel, ServerSocketChannel}
import com.twitter.ostrich.stats.Stats

/**
 *
 * User: light
 * Date: 06/02/14
 * Time: 19:16
 */
class PipedOutputData(conf: ClusterConfigs, dir: File) {

  val uuid = new UUID().toString
  val tmpFile = new File(dir, uuid + ".pipeOut.tmp")
  tmpFile.createNewFile()
  tmpFile.deleteOnExit()

  val log = LoggerFactory.getLogger(getClass.getName + "#" + uuid.substring(0, 10) )

  val isFinished = new AtomicBoolean(false)
  val availableBytes = new AtomicLong(0)

  val socket = ServerSocketChannel.open()

  val completionPromise = Promise[CompletionResult]()

  PipedTransfer.pipesOut.incrementAndGet()

  val port = {
    val s = socket.bind(new InetSocketAddress(conf.publicAddress, 0))
    s.getLocalAddress.asInstanceOf[InetSocketAddress].getPort
  }

  def address = new InetSocketAddress(conf.publicAddress, port)


  val lock = new ReentrantLock()
  val cond = lock.newCondition()

  var crBuffer = 0
  var crBytes = 0

  val bb = new MMFByteBuf(tmpFile, PipedTransfer.BUFFER_STEP)

  val workerThread = new Thread() {
    override def run() = {
      try {
        val s = socket.accept()

        try {
//          log.debug("PIPE: connected: " + s.getRemoteAddress)

          var isCloseRequested = false
          val ch = ByteBuffer.allocate(4)

          while (!isCloseRequested) {
            val count = s.read(ch)

            if (count == -1) {
              isCloseRequested = true

              try {
                s.close()
              } catch {
                case x: Exception =>
              }

              if (!completionPromise.isCompleted)
                completionPromise.success(CompletionResults.FAIL_CLIENT_CONNECT)
            } else {

              ch.flip()
              if (ch.remaining() != 4) {
                throw new IllegalStateException("Impossible ? : " + ch.remaining())
              }

              val code = ch.getInt
              ch.clear()

              code match {
                case 1 =>
                  // read
                  onSocketReady(s)

                case 2 =>
                  // force close
                  isCloseRequested = true
                  completionPromise.success(CompletionResults.FAIL_CLIENT)

                case 3 =>

                  isCloseRequested = true

                  if (!isFinished.get()) {
                    throw new IllegalStateException("! finished, but close requested")
                  }

                  completionPromise.success(CompletionResults.OK)
              }
            }
          }

        } finally {
          try { s.close() } catch { case x:Exception => }
        }

      } catch {
        case x: Exception =>
          log.warn("Failed", x)

          if (! completionPromise.isCompleted) {
            completionPromise.failure(x)
          }

      } finally {

        // bb.forceClose()
        closePipe()
      }
    }
  }


  def closePipe() {
//    log.debug("PIPE CLOSING...")

    PipedTransfer.pipesOut.decrementAndGet()

    try {
      bb.close()
    } catch {
      case x: Exception =>
    }

    try {
      socket.close()
    } catch {
      case x: Exception =>
    }

    try {
       tmpFile.delete()
    } catch {
      case x: Exception =>
    }

//    log.debug("PIPE CLOSED")

    if (!completionPromise.isCompleted) {
      completionPromise.failure(new RuntimeException("Server Code Error"))
    }
  }

  workerThread.start()

//  log.debug("Pipe created: onAddr:" + socket.getLocalAddress + " / file:" + tmpFile.getPath)

  def write(datas: BIN*) {
    var idx = 0
    lock.lock()
    try {
      var len = 0

      while (idx < datas.length) {
        val data = datas(idx)
        bytesWritten += data.length

        val l = data.length + 4

        if (l > PipedTransfer.BUFFER_STEP) {
          throw new BufferOverflowException()
        }

        len += l

        bb.writeInt(data.length)
        bb.write(data)

        idx += 1
      }

      availableBytes.addAndGet(len)
      cond.signalAll()
    } finally {
      lock.unlock()
    }
  }

  var bytesWritten : Long = 0

  def write(data : ByteBuffer) {

    lock.lock()
    try {
      bytesWritten += data.remaining()

      val len = data.remaining() + 4

      if (len > PipedTransfer.BUFFER_STEP) {
        throw new BufferOverflowException()
      }

      bb.writeInt(data.remaining())
      bb.write(data)

      availableBytes.addAndGet(len)
      cond.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def setFinish() {
    lock.lock()
    try {

//      log.debug("PIPE FINISHED: " + bytesWritten)
      isFinished.set(true)
      cond.signalAll()

    } finally {
      lock.unlock()
    }
  }

  def setFailed(x : Exception) {
    log.warn("Close pipe by error: ", x)

    Stats.incr("pipes/out/error/" + x.getClass.getSimpleName)

    lock.lock()
    try {
      isFinished.set(true)
      cond.signalAll()

//      closePipe()

      try { workerThread.interrupt() } catch { case x:Exception => }
    } finally {
      lock.unlock()
    }

  }

  def onSocketReady(sc: SocketChannel) {
    lock.lock()
    try {

      if ( availableBytes.get() == 0 && isFinished.get() ) {
//        println("SEND -1")
        sc.write( ByteBuffer.wrap(PrimitiveBits.intToBytes(-1)) ) // should close
      } else {

        while (availableBytes.get() == 0 && !isFinished.get()) {
          cond.await(100, TimeUnit.MILLISECONDS)
        }

        while (availableBytes.get() > 0) {
          if (bb.getWrittenBytes(bb.getBuffersDirty()(crBuffer)) == crBytes) {

            bb.cleanup(crBuffer)

            crBytes = 0
            crBuffer += 1
          }

          val avail = math.min(availableBytes.get(), PipedTransfer.PACKET_SIZE).toInt

          val currentBuffer = bb.getBuffersDirty()(crBuffer)
          val bytesWritten = bb.getWrittenBytes(currentBuffer)
          val bytesRemains = bytesWritten - crBytes

          val toReadFromThisBuffer = math.min(bytesRemains, avail)

          val pp = currentBuffer.position()
          currentBuffer.position(crBytes)

          val s = currentBuffer.slice()
          s.limit(toReadFromThisBuffer)

          currentBuffer.position(pp)

//          println("SEND DATA: " + s.remaining())

          sc.write( ByteBuffer.wrap(PrimitiveBits.intToBytes( s.remaining() )) )
          sc.write( s )

          crBytes += toReadFromThisBuffer
          availableBytes.addAndGet(-toReadFromThisBuffer)

        }
      }


    } finally {
      lock.unlock()
    }
  }
}
