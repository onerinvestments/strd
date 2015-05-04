package strd.util

import java.util.concurrent.{TimeoutException, TimeUnit, Executors}
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.duration.Duration
import strd.trace.PoolContext
import strd.concurrent.Promise


/**
 *
 * User: light
 * Date: 18/02/14
 * Time: 17:53
 */

object FutureTimer {
  val sched = Executors.newSingleThreadScheduledExecutor()
  implicit val executionContext = PoolContext.cachedExecutor()

  def apply[X](f : Future[X], duration : Duration) : Future[X] = apply(f,duration.toMillis)

  def apply[X](f : Future[X], timeout:Long, modifyStackTrace : Boolean = true) : Future[X] = {

    val p = Promise[X]()
    val stackTrace = if (modifyStackTrace) Thread.currentThread().getStackTrace else null

    val task = new Runnable {
      override def run()  {
        val x = new TimeoutException()

        if (modifyStackTrace)
          x.setStackTrace(stackTrace)

        p.failure(x)
      }
    }

    val s = sched.schedule(task, timeout, TimeUnit.MILLISECONDS)

    f.onComplete{
      case Success(x) =>
        try { s.cancel(false) } catch { case t:Exception =>}
        p.trySuccess(x)
      case Failure(x:Throwable) =>
        try { s.cancel(false) } catch { case t:Exception =>}
        p.tryFailure(x)
    }

    p.future
  }

}
