package strd.client

import strd.db.StrdDbEnvironment
import strd.dbv2.proto.StrdDbDsl.{StrdDbTableSchema, DbEntity}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import java.util.concurrent.{Semaphore, ConcurrentHashMap}
import com.twitter.ostrich.admin.Service
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.convert.Wrappers.JConcurrentMapWrapper
import scala.collection.mutable.ArrayBuffer
import java.io.ByteArrayOutputStream
import strd.core.StrdPackage
import strd.util._
import lmbrd.zn.util.TimeUtil
import akka.pattern._
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.{TimerTask, Timer}
import org.slf4j.LoggerFactory
import com.twitter.ostrich.stats.Stats
import strd.dbv2.proto.StrdDb.StrdDbWriteBatch
import akka.util.Timeout
import scala.collection.convert.Wrappers.JConcurrentMapWrapper
import strd.core.StrdPackage
import strd.cluster.DNConf

/**
 *
 * User: light
 * Date: 11/08/14
 * Time: 13:11
 */

class StrdDbEnvironmentImpl(implicit val bindingModule: BindingModule) extends StrdDbEnvironment with Injectable with Service{

  val tableSender = inject[NodesClient]('dataNodes)

  val tables = new ConcurrentHashMap[Int, TableWriteBuffer]()
  val timer  = new Timer()
  val running = new AtomicBoolean(true)

  implicit val conf   = inject[ClusterConfigs]
  val FLUSH_INTERVAL  = DNConf(_.FLUSH_INTERVAL)
  val CHUNK_SIZE      = math.max( (DNConf(_.RECORDS_TO_FLUSH) / 2), 100 )

  implicit val executionContext = inject[ExecutionContext]

  val log = LoggerFactory.getLogger(getClass)

  override def quiesce() = {
    running.set(false)
    timer.cancel()
  }

  override def shutdown() = {
    JConcurrentMapWrapper( tables ).values.foreach(_.flushSync())
  }

  override def start() = {
    timer.schedule(new TimerTask(){
      override def run() = {
        try {
          JConcurrentMapWrapper( tables ).values.foreach( t => {
            t.synchronized {
              if ( t.timeSinceLastFlush > 20 * TimeUtil.aSECOND || t.bufferSize >= CHUNK_SIZE ) {
                t.flush()
              }
            }
          })

        } catch {
          case x: Exception =>
            log.error("Failed",x)
        }
      }
    }, 100, 100)
  }

  override def storeBatch(e: Iterable[DbEntity], schema: StrdDbTableSchema) = {
    sender(schema).storeBatch(e)
  }

  override def store(e: DbEntity, schema: StrdDbTableSchema) = {
    sender(schema).store(e)
  }

  override def flushSync(tableId: Int) = {
    Option( tables.get(tableId) ).foreach(_.flushSync())
  }

  private def sender( schema : StrdDbTableSchema) = {
    if (! running.get()) {
      throw new RuntimeException("Drop event, we are stipping now")
    }

    val id : Int = schema.getTableId

    var buf = tables.get(id)
    if (buf == null) {
      tables.synchronized{
        buf = tables.get(id)
        if (buf == null) {
          buf = new TableWriteBuffer(id, schema)
          tables.put(id, buf)
        }
      }
    }

    buf
  }

  class TableWriteBuffer(val tableId : Int,
                         val schema : StrdDbTableSchema) {

    @volatile var buffer = new ArrayBuffer[DbEntity](1000)
    var lastFlush = CTime.now

    def storeBatch(iterable: Iterable[DbEntity]) = synchronized {

      buffer ++= iterable

      if (bufferSize >= CHUNK_SIZE) {
        flush()
      }
    }

    def store(entity: DbEntity) = synchronized {
      buffer += entity

      if (bufferSize >= CHUNK_SIZE) {
        flush()
      }
    }

    def flushSync() {
      try {
        Await.result(flush(), 1 minute)
      } catch {
        case x: Exception => log.warn("failed", x)
      }
    }

    def timeSinceLastFlush = synchronized { CTime.now - lastFlush }

    def bufferSize = synchronized { this.buffer.size }

    private [client] def flush() : Future[Any] = synchronized {

      try {
        lastFlush = CTime.now

        if (this.buffer.size == 0) {
          return Future.successful(true)
        }

        val buf = this.buffer
        this.buffer = new ArrayBuffer[DbEntity](1000)


        val batch = StrdDbWriteBatch.newBuilder().setTable(schema)

        buf.foreach(batch.addEntities)

        log.info( s"DB $tableId WRITE_PACKAGE: ${batch.getEntitiesCount} entries")

        implicit val timeout = Timeout(1 * TimeUtil.aMINUTE)
        val future = tableSender.dispatcher ? StrdPackage(MonotonicallyIncreaseGenerator.nextId(), buf.size, batch.build().toByteArray, 1 * TimeUtil.aMINUTE)

        future

      } catch {
        case x: Exception =>
          Future.failed(x)
      }
    }

  }

}
