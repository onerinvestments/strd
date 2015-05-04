package strd.concurrent

import scala.concurrent.duration.{FiniteDuration, Deadline, Duration}
import scala.annotation.tailrec
import scala.concurrent._
import scala.util.Try
import scala.concurrent.TimeoutException
import scala.util.Failure
import scala.Some
import java.util.concurrent.ExecutionException
import scala.util.Success
import scala.util.control.NonFatal
import strd.trace.{TraceContext, PoolContext}

/**
 *
 * User: light
 * Date: 14/05/14
 * Time: 20:07
 */

object Promise {

  def apply[T](): scala.concurrent.Promise[T] = new TracePromise[T](PoolContext.threadLocal.get())

  def apply[T](ctx: TraceContext): scala.concurrent.Promise[T] = new TracePromise[T](ctx)


  class TracePromise[T](val ctx: TraceContext) extends strd.concurrent.AbstractPromise with scala.concurrent.Promise[T] with scala.concurrent.Future[T] {
    self =>
    updateState(null, Nil)

    // Start at "No callbacks"

    override def future = this

    protected final def tryAwait(atMost: Duration): Boolean = {
      @tailrec
      def awaitUnsafe(deadline: Deadline, nextWait: FiniteDuration): Boolean = {
        if (!isCompleted && nextWait > Duration.Zero) {
          val ms = nextWait.toMillis
          val ns = (nextWait.toNanos % 1000000l).toInt // as per object.wait spec

          synchronized {
            if (!isCompleted) wait(ms, ns)
          }

          awaitUnsafe(deadline, deadline.timeLeft)
        } else
          isCompleted
      }
      @tailrec
      def awaitUnbounded(): Boolean = {
        if (isCompleted) true
        else {
          synchronized {
            if (!isCompleted) wait()
          }
          awaitUnbounded()
        }
      }

      import Duration.Undefined
      atMost match {
        case u if u eq Undefined => throw new IllegalArgumentException("cannot wait for Undefined period")
        case Duration.Inf => awaitUnbounded
        case Duration.MinusInf => isCompleted
        case f: FiniteDuration => if (f > Duration.Zero) awaitUnsafe(f.fromNow, f) else isCompleted
      }
    }

    @throws(classOf[TimeoutException])
    @throws(classOf[InterruptedException])
    def ready(atMost: Duration)(implicit permit: CanAwait): this.type =
      if (isCompleted || tryAwait(atMost)) this
      else throw new TimeoutException("Futures timed out after [" + atMost + "]")

    @throws(classOf[Exception])
    def result(atMost: Duration)(implicit permit: CanAwait): T =
      ready(atMost).value.get match {
        case Failure(e) => throw e
        case Success(r) => r
      }

    def value: Option[Try[T]] = getState match {
      case c: Try[_] => Some(c.asInstanceOf[Try[T]])
      case _ => None
    }

    override def isCompleted: Boolean = getState match {
      // Cheaper than boxing result into Option due to "def value"
      case _: Try[_] => true
      case _ => false
    }

    def tryComplete(value: Try[T]): Boolean = {
      val resolved = resolveTry(value)
      (try {
        @tailrec
        def tryComplete(v: Try[T]): List[CallbackRunnable[T]] = {
          getState match {
            case raw: List[_] =>
              val cur = raw.asInstanceOf[List[CallbackRunnable[T]]]
              if (updateState(cur, v)) cur else tryComplete(v)
            case _ => null
          }
        }
        tryComplete(resolved)
      } finally {
        synchronized {
          notifyAll()
        } //Notify any evil blockers
      }) match {
        case null => false
        case rs if rs.isEmpty => true
        case rs => rs.foreach(r => r.executeWithValue(resolved)); true
      }
    }

    def onComplete[U](func: Try[T] => U)(implicit executor: ExecutionContext): Unit = {
      val preparedEC = executor.prepare
      val runnable = new CallbackRunnable[T](preparedEC, func, ctx)

      @tailrec //Tries to add the callback, if already completed, it dispatches the callback to be executed
      def dispatchOrAddCallback(): Unit =
        getState match {
          case r: Try[_] => runnable.executeWithValue(r.asInstanceOf[Try[T]])
          case listeners: List[_] => if (updateState(listeners, runnable :: listeners)) () else dispatchOrAddCallback()
        }
      dispatchOrAddCallback()
    }
  }


  private def resolveTry[T](source: Try[T]): Try[T] = source match {
    case Failure(t) => resolver(t)
    case _ => source
  }

  private def resolver[T](throwable: Throwable): Try[T] = throwable match {
    case t: scala.runtime.NonLocalReturnControl[_] => Success(t.value.asInstanceOf[T])
    case t: scala.util.control.ControlThrowable => Failure(new ExecutionException("Boxed ControlException", t))
    case t: InterruptedException => Failure(new ExecutionException("Boxed InterruptedException", t))
    case e: Error => Failure(new ExecutionException("Boxed Error", e))
    case t => Failure(t)
  }

}

/* Precondition: `executor` is prepared, i.e., `executor` has been returned from invocation of `prepare` on some other `ExecutionContext`.
 */
private class CallbackRunnable[T](val executor: ExecutionContext,
                                  val onComplete: Try[T] => Any,
                                  val ctx: TraceContext) extends Runnable with OnCompleteRunnable {

  // must be filled in before running it
  var value: Try[T] = null

  override def run() = {
    require(value ne null) // must set value to non-null before running!
    try onComplete(value) catch {
      case NonFatal(e) => executor reportFailure e
    }
  }

  def executeWithValue(v: Try[T]): Unit = {
    val c = PoolContext.threadLocal.get()
    try {

      PoolContext.threadLocal.set(ctx)

      require(value eq null) // can't complete it twice
      value = v
      // Note that we cannot prepare the ExecutionContext at this point, since we might
      // already be running on a different thread!
      try executor.execute(this) catch {
        case NonFatal(t) => executor reportFailure t
      }

    } finally {
      PoolContext.threadLocal.set(c)
    }
  }
}
