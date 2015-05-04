package strd.concurrent

import java.util.concurrent.TimeoutException

import strd.trace.PoolContext

import scala.concurrent.Future

/**
 * @author Kirill chEbba Chebunin
 */
class WorkerPool(name: String, cl: ClassLoader, ids: Seq[Int], consumers: Int = 1, queueSize: Int = 1024) {
  val TIMEOUT = 1000

  private val workers = ids.map(id => id -> new Worker(id, cl, name, consumers, queueSize)).toMap

  def hasWorker(id: Int) = workers.contains(id)

  def workerIds = workers.keys

  def size = workers.size

  def updateClassLoader(cl : ClassLoader) {
    workers.foreach(_._2.updateClassLoader(cl))
  }

  def submitTask[X](workerId: Int)(h: => X ): Future[X] = {
    val p = strd.concurrent.Promise[X]()
    workers.get(workerId).map { w =>
      val t = new Task[X](p, PoolContext.get, h)

      if (!w.isAlive) {
        p.tryFailure(new RuntimeException(s"Worker $workerId shutdown"))
      }

      val offerResult = w.submitTask(t, TIMEOUT)

      if (! offerResult) {
        p.tryFailure(new TimeoutException(s"Time out while submitting to worker $workerId"))
      }

    }.getOrElse {
      p.tryFailure(new RuntimeException(s"Worker $workerId is unavailable") )
    }

    p.future
  }

  def foreach[X](h: Worker => X ) : Map[Int, Future[X]] = {
    workers.values.map { w =>
      w.id -> submitTask(w.id)(h(w))
    }.toMap
  }

  def shutdown(await: Boolean = false) {
    workers.foreach(_._2.shutdown(await))
  }
}
