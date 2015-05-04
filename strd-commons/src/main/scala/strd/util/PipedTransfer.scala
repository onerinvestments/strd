package strd.util

import java.util.concurrent.atomic.AtomicInteger
import strd.dht.BIN
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.{ConcurrentSkipListSet, CopyOnWriteArraySet}


/**
 *
 * User: light
 * Date: 06/02/14
 * Time: 15:05
 */

object PipedTransfer {
  val BUFFER_STEP = 16 * 1024 * 1024   // 16MB
  val PACKET_SIZE = 4  * 1024 * 1024   // 4Mb

  val pipesOut = new AtomicInteger()
  val pipesIn = new AtomicInteger()

  val pipes = new ConcurrentSkipListSet[String]()

  def addPipe(uuid : String) {
    pipes.add(uuid)
    Stats.setLabel("pipes/progress", pipes.toString)
  }

  def removePipe(uuid : String) {
    pipes.remove(uuid)
    Stats.setLabel("pipes/progress", pipes.toString)
  }

  Stats.addGauge("pipes/in/open") {
    pipesIn.get()
  }

  Stats.addGauge("pipes/out/open") {
    pipesOut.get()
  }
}

trait CompletionResult { }

object CompletionResults {
  case object OK extends CompletionResult

  case object FAIL_CLIENT_CONNECT extends CompletionResult

  case object FAIL_SERVER extends CompletionResult

  case object FAIL_CLIENT extends CompletionResult
}
class PipedEntry( ) {
  var data : BIN = null
  var length : Int = 0
}





