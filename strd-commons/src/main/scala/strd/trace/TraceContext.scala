package strd.trace

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.google.protobuf.Message
import com.twitter.ostrich.stats.Stats
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import strd.util.CTime

import scala.concurrent.{Future, _}
import scala.util.{Failure, Success}

/**
 *
 * User: light
 * Date: 13/05/14
 * Time: 17:54
 */
object Helpers {
  val v1 = Array(":", "/", " ")
  val v2 = Array("_", ".", "_")

  def safeName(str: String) = StringUtils.replace(StringUtils.replaceEach(str.toLowerCase, Helpers.v1, Helpers.v2), "..", ".")
}

object TraceEntry {
  def apply(  name: String,
              path: String,
              reqReceived: Long ): TraceEntry = {
    new TraceEntry(Helpers.safeName(name), path, reqReceived )
  }
}
case class TraceMessage(msg : String, timeLocal : Int)
class TraceEntry(val name: String,
                 val path: String,
                 val reqStarted: Long ) {


  def fullPath = if (! path.isEmpty) path + "." + name else name

  var children: Seq[TraceEntry] = Nil
  var msgs: Seq[TraceMessage] = Nil

  def push(name: String) = {
    //val now = CTime.now
    val child = TraceEntry(name, if (path.isEmpty) name else path + "." + name, reqStarted)
    children = children :+ child
    child
  }

  def append(msg: String, time : Long) = synchronized {
    msgs = msgs :+ TraceMessage(msg,  (time - this.reqStarted).toInt )
  }

  def mkString(i: Int = 1): String = {

    val prefix = "  " * i
    val sb = new StringBuilder("--" * i).append(" ").append(name).append("\n")

    msgs.foreach(x => sb.append(prefix).append(x).append("\n"))
    children.map(ch => sb.append(ch.mkString(i + 1)))
    sb.toString()
  }
}

trait TraceSubmitter {
  def appendTrace(ctx: TraceEntry)
}

case class TraceContext(
                         submitter: TraceSubmitter,
                        trace: TraceEntry,
                        traceLevel: Int) {
  var remotes : Int = 0

  def remoteInvoke: Int = synchronized{
    remotes += 1
    remotes
  }


  val root = trace


  def submitReply(value: Message) {
    // TODO:
    submitResult()
  }

  def submitResult() {
    submitter.appendTrace(root)
  }


  def appendMessage(msg: String) {
    trace.append(msg, CTime.now)
  }

  def child(name: String) = {
    copy(trace = trace.push(name))
  }


}


object PoolContext {
  var traceSubmmiter: Option[TraceSubmitter] = None

  def get: TraceContext = threadLocal.get()

  def opt = Option(get)

  def threadFactory( name : String, priority : Int) = {
    new ThreadFactory() {
      val counter = new AtomicInteger()

      override def newThread(r: Runnable) = {
        val t = new Thread(r, name + "-" + counter.incrementAndGet())
        if (t.isDaemon) t.setDaemon(false)
        t.setPriority(priority)
        t
      }
    }
  }

  def cachedExecutor(count : Int = Runtime.getRuntime.availableProcessors(),
                     name  : String = "ctxpl",
                     priority : Int = Thread.NORM_PRIORITY,
                     qSz : Int = 10000) = {

    val queue = new ArrayBlockingQueue[Runnable](qSz)

    Stats.addGauge("pool/" + name + "_" + count) {
      queue.size()
    }

    val executor = new ThreadPoolExecutor(count, count, 0L, TimeUnit.MILLISECONDS, queue, threadFactory(name, priority))

    fromExecutor( name, executor )
  }


  def fromExecutor(name : String, exec: Executor) = {
    new TracedExecutionContext(name, ExecutionContext.fromExecutor(exec))
  }


  def notifySubmitter(ctx: TraceEntry) = {
    traceSubmmiter.map {
      s =>
        try {
          s.appendTrace(ctx)
        } catch {
          case ignored: Exception =>
            ignored.printStackTrace()
        }
    }
  }

  def trace[X](rootName: String, level: Int)(h: => X): X = {
    if (traceSubmmiter.isDefined) {
      val ctx = TraceContext(traceSubmmiter.get, TraceEntry(rootName, "", CTime.now), level)
      threadLocal.set(ctx)

      try {
        h
      } finally {
        ctx.submitResult()
        threadLocal.remove()
      }
    } else {
      h
    }
  }

  // simple pool
  private implicit val exectionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def futureStopTrace(f : Future[_], ctx : TraceContext) = {
    if (traceSubmmiter.isDefined) {
      f.onComplete {
        case Success(r) =>
          ctx.submitResult()

        case Failure(t) =>
          ctx.appendMessage("! Future failed, " + t.getMessage)
          ctx.submitResult()
      }
    }
    f
  }

  def clear() {
    threadLocal.remove()
  }

  val threadLocal = new ThreadLocal[TraceContext]

  def traceOutput(minLevel: Int)(h: => String) {
    val ctx = threadLocal.get()
    if (ctx != null && ctx.traceLevel >= minLevel) {
      val result = h
      ctx.appendMessage(result)
    }
  }

  def traceLevel : Int = {
    val ctx = threadLocal.get()
    if (ctx == null ) 0
    else ctx.traceLevel
  }



  def startTrace(name: String, level : Int): Option[TraceContext] = {
    traceSubmmiter.map( s=> {
      val ctx = TraceContext(s, TraceEntry(name, "", CTime.now), level)
      threadLocal.set(ctx)
      ctx
    } )
  }

  def stopTrace(ctx : TraceContext) {
    ctx.submitResult()
    threadLocal.remove()
  }

  def stopTrace(future : Future[_], ctx : TraceContext) {
    futureStopTrace(future, ctx)
    threadLocal.remove()
  }

}



class TracedExecutionContext(name : String, exec: ExecutionContext) extends ExecutionContext {
  //var exec = exec1.prepare()

  val log = LoggerFactory.getLogger(getClass)


  override def reportFailure(t: Throwable) = {
    exec.reportFailure(t)
  }

  override def execute(runnable: Runnable) = {
    try {
      val ctx = PoolContext.threadLocal.get()

      if (ctx != null) {
        exec.execute( new RunWithContext(ctx, runnable) )
      } else {
        exec.execute( runnable )
      }
    } catch {
      case x : Exception =>
        log.error(s"Unexpected error in $name", x)
        reportFailure(x)
        throw x
    }
  }

  class RunWithContext( ctx : TraceContext, runnable : Runnable) extends Runnable {
    override def run() = {
      PoolContext.threadLocal.set(ctx)
      try {
        runnable.run()
      } finally {
        PoolContext.threadLocal.remove()
      }
    }
  }


}


class TraceLogAppender extends AppenderBase[ILoggingEvent] {
  def append(eventObject: ILoggingEvent) {
    val ctx = PoolContext.threadLocal.get()
    if (ctx != null) {
      ctx.appendMessage(s"${eventObject.getLevel.levelStr.toLowerCase}: ${StringUtils.replace( eventObject.getLoggerName, "." ,"_" )}    ${eventObject.getMessage}")
    }
  }
}

