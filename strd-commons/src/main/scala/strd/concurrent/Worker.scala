package strd.concurrent

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.slf4j.LoggerFactory
import strd.trace.{PoolContext, TraceContext}

import scala.concurrent.Future
import scala.util.Try

/**
 * @author Kirill chEbba Chebunin
 */
class Worker(val id: Int, initCl: ClassLoader, group: String, consumers: Int = 1, queueSize: Int = 1024) {
  val log = LoggerFactory.getLogger(getClass)

  val q = new ArrayBlockingQueue[Task[_]](queueSize)

  val clRef = new AtomicReference[ClassLoader](initCl)
  val shutdownFlag = new AtomicInteger(0)
  val startSemaphore = new Semaphore(0)
  val stopSemaphore = new Semaphore(0)

  val currentThread = new AtomicReference[ThreadSet](createThread(initCl))
  currentThread.get().start()
  awaitBootstrap()

  def updateClassLoader(cl : ClassLoader) {
    log.debug(s"Task queue contains ${q.size}")
    clRef.set(cl)
    shutdownFlag.set(1)

//    currentThread.get().interrupt()
    awaitBootstrap()
  }

  def submitTask(task : Task[_], timeout : Int): Boolean = {
    q.offer(task, timeout, TimeUnit.MILLISECONDS)
  }

  def shutdown(await: Boolean = false) {
    import scala.collection.JavaConverters._

    shutdownFlag.set(2)

    val tasks = new java.util.ArrayList[Task[_]]()
    q.drainTo(tasks)

    log.warn(s"Purging ${tasks.size} tasks")
    tasks.asScala.foreach(_.p.tryFailure(new RuntimeException("Worker shutdown")))

    if (await) {
      log.debug(s"Worker $id waiting to stop")
      if (!stopSemaphore.tryAcquire(consumers, 3, TimeUnit.MINUTES)) {
        log.warn(s"Worker $id failed to stop all consumers")
      } else {
        log.debug(s"Worker $id stopped")
      }
    }
  }

  def isAlive = shutdownFlag.get() != 2

  def createThread(cl : ClassLoader) : ThreadSet = {
    val threads =
      0.until(consumers).map { i =>
        val th = new Thread(s"worker-$group-${id}_$i") {
          override def run() = {
            println(s"Started thread: $id/$i")
            Worker.id.set(id)

            startSemaphore.release()

            try {
              while (!Thread.currentThread().isInterrupted && shutdownFlag.get() == 0) {
                val r = q.poll(10, TimeUnit.MILLISECONDS)

                if (r != null) {
                  try {
                    val x = r.run()
                    r.p.asInstanceOf[scala.concurrent.Promise[Any]].trySuccess(x)
                  } catch {
                    case x: Exception => r.p.tryFailure(x)
                  }
                }
              }
            } catch {
              case e: Exception => log.warn(s"Unexpected exception in worker $id/$i", e)
            }
            println(s"Finished thread: $id/$i")
            stopSemaphore.release()
            workerExit()
          }
        }

        th.setContextClassLoader(cl)
        println("Created thread: " + id + " / " + i)
        th
      }.toArray[Thread]

    new ThreadSet( threads )
  }

  def awaitBootstrap() {
    if (!startSemaphore.tryAcquire(consumers, 15, TimeUnit.SECONDS) ) {
      throw new IllegalStateException("Not all consumers were started")
    }
  }

  def workerExit() {
    println("Worker exit")
    if (shutdownFlag.compareAndSet(1, 0)) {
      println("Switch classLoader")
      val newThread = createThread(clRef.get())
      currentThread.set(newThread)
      newThread.start()
    }
  }
}

object Worker {
  val id = new ThreadLocal[Int]()

  def test[X](cl: ClassLoader)(h: => X) : Future[X] = {
    val p = strd.concurrent.Promise[X]()
    val th = new Thread() {
      override def run() {
        p.complete(Try(h))
      }
    }
    th.setContextClassLoader(cl)
    th.start()

    p.future
  }
}

class Task[X]( val p : scala.concurrent.Promise[X], ctx : TraceContext,  h : => X ) {
  def run(): X = {
    PoolContext.threadLocal.set(ctx)
    try {
      h
    } finally {
      PoolContext.threadLocal.remove()
    }
  }


}

class ThreadSet(threads: Array[Thread]) {
  def start(): Unit = {
    threads.foreach(_.start())
  }

  def interrupt() : Unit = {
    threads.foreach(_.interrupt())
  }
}
