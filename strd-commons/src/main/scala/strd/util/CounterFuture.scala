package strd.util

import strd.concurrent.Promise
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * User: light
 * Date: 9/30/13
 * Time: 1:00 AM
 */
case class CounterFuture( await : Int ) {
  val p = Promise[Int]()
  var counter :Int = 0
  var success :Int = 0

  def append(d:Int = 1) = {
    this.synchronized{
      counter +=1
      success += 1

      if ( counter == await) {

        if (success == await) {
          p.success(await)
        } else {
          setFail()
        }
      }
    }
  }

  private def setFail() {
    p.failure( new RuntimeException(s"Success count $success / $counter"))
  }

  def error() = {
    synchronized {
      counter+=1
      if ( counter == await ) {
        setFail()
      }
    }

  }

  def future = p.future
}
