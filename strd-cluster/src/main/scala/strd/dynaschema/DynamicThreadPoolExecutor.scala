package strd.dynaschema

import org.slf4j.LoggerFactory
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import scala.concurrent.ExecutionContext
import strd.trace.PoolContext

/**
 *
 * User: light
 * Date: 15/04/14
 * Time: 16:28
 */

class DynamicThreadPoolExecutor(schemaName: String,
                                cps: Int = 0,
                                mps: Int = 1024,
                                ka: Int = 60,
                                tunit: TimeUnit = TimeUnit.SECONDS,
                                queue: BlockingQueue[Runnable] = new SynchronousQueue[Runnable]())

  extends ThreadFactory with Executor {

  val exec = PoolContext.fromExecutor(schemaName +"P", this)

  val log = LoggerFactory.getLogger(getClass)
  val counter = new AtomicInteger()
  val currentClassLoader = new AtomicReference[ClassLoader]()
  val currentPool = new AtomicReference[ThreadPoolExecutor]()

  def updateClassLoader(cl: ClassLoader) = {
    log.info("Set classLoader :" + cl)

    val oldClassLoader = this.currentClassLoader.getAndSet(cl)
    val newPool = createThreadPool

    log.info("shutdown pool threadPool")
    Option(this.currentPool.getAndSet(newPool)).foreach(oldPool => oldPool.shutdown())

  }

  override def execute(command: Runnable) = {
    var e = currentPool.get()
    if (e == null) {
      synchronized {
        e = currentPool.get()
        if (e == null) {
          e = createThreadPool
          currentPool.set(e)
        }
      }
    }

    e.execute(command)
  }

  override def newThread(r: Runnable) = {
    val cl = currentClassLoader.get()
    if (cl == null) {
      throw new NullPointerException("no classloader")
    }

    val th = new Thread(r, "dth-" + schemaName + "_" + counter.getAndIncrement)
    th.setContextClassLoader(cl)

    th
  }

  def createThreadPool = {
    new ThreadPoolExecutor(cps, mps, ka, tunit, queue, this)
  }

}

