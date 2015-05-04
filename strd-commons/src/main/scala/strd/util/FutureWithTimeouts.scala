package strd.util

import java.util.concurrent.{TimeUnit, Executors}
import scala.concurrent.{ExecutionContext, TimeoutException,  Future}
import scala.util.{Success, Failure}
import strd.concurrent.Promise

/**
 *
 * User: light
 * Date: 09/04/14
 * Time: 17:24
 */

object FutureWithTimeouts {
  val scheduler = Executors.newScheduledThreadPool(2)


}

class FutureWithTimeout[X](f : Future[X]) {

  def withTimeout( timeout : Long, modifyStackTrace : Boolean = true)(implicit executionContext : ExecutionContext) : Future[X] = {
    val p = Promise[X]()
    if (timeout < 0) {
      p.tryFailure( new TimeoutException("Future timed out:" + timeout) )
    } else {
      val stackTrace = if (modifyStackTrace) Thread.currentThread().getStackTrace else null

      val scheduled = FutureWithTimeouts.scheduler.schedule(
        new Runnable() {
          override def run() = {
            val exception = new TimeoutException("Future timed out:" + timeout)

            if (modifyStackTrace)
              exception.setStackTrace(stackTrace)

            p.tryComplete(Failure(exception))
          }
        }, timeout, TimeUnit.MILLISECONDS)

      f.onComplete {
        case s: Success[X] =>
          scheduled.cancel(false)
          p.tryComplete(s)

        case th: Failure[X] =>
          scheduled.cancel(false)
          p.tryComplete(th)
      }
    }

    p.future
  }

}
