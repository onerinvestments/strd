package strd.client

import java.util.Properties

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.twitter.ostrich.admin.Service
import com.twitter.ostrich.stats.Stats
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import lmbrd.zn.util.PrimitiveBits
import org.apache.curator.framework.CuratorFramework
import org.slf4j.LoggerFactory
import strd.db.StrdDbEnvironment
import strd.dbv2.proto.StrdDbDsl.{DbEntity, StrdDbTableSchema}
import strd.kafka.KafkaConf
import strd.runtime.{TableRegistry, TableUtils}
import strd.util._

/**
 *
 * User: light
 * Date: 11/08/14
 * Time: 13:11
 */

class StrdDbKafkaEnvironmentImpl(implicit val bindingModule: BindingModule) extends StrdDbEnvironment with Injectable with Service {

  implicit val conf = inject[ClusterConfigs]
  val tableRegistry = inject[TableRegistry]
  val log = LoggerFactory.getLogger(getClass)

  val curator = inject[CuratorFramework]
  val props = new Properties()

  private val brokersList = conf.nodeForType("strd-kafka").map(n => s"${n.addr}:${KafkaConf(_.KAFKA_BROKER_PORT)}").mkString(",")

  log.debug("producer broker list: " + brokersList)

  props.put("metadata.broker.list", brokersList)
  props.put("serializer.class", "strd.client.DbEntityEncoder")
  props.put("key.serializer.class", "strd.client.LongEncoder")
  props.put("producer.type", "async")

  val config = new ProducerConfig(props)
  val producer = new Producer[Long, DbEntity](config)


  def sendEvent(e: DbEntity, tableId: Int) = {
    producer.send(new KeyedMessage(TableUtils.tableTopic(tableId), conf.nextId(), e))
  }


  override def start() = {

  }

  override def shutdown() = {
    producer.close()
  }


  override def flushSync(tableId: Int) = {
  }

  override def store(e: DbEntity, schema: StrdDbTableSchema) = {
    Stats.incr(s"kafka/send/${schema.getTableId}")
    if(!tableRegistry.tableExists(schema.getTableId)) {
      throw new IllegalStateException("no table topic " + schema.getTableId)
    }
    producer.send(new KeyedMessage(TableUtils.tableTopic(schema.getTableId), conf.nextId(), e))
  }

  override def storeBatch(e: Iterable[DbEntity], schema: StrdDbTableSchema) = {
    Stats.incr(s"kafka/send/${schema.getTableId}", e.size)
    if(!tableRegistry.tableExists(schema.getTableId)) {
      throw new IllegalStateException("no table topic " + schema.getTableId)
    }
    producer.send(e.map(e => new KeyedMessage(TableUtils.tableTopic(schema.getTableId), conf.nextId(), e)).toSeq: _*)
  }

}

class DbEntityEncoder(props: VerifiableProperties = null) extends Encoder[DbEntity] {
  override def toBytes(t: DbEntity) = t.toByteArray
}

class LongEncoder(props: VerifiableProperties = null) extends Encoder[Long] {
  override def toBytes(t: Long) = PrimitiveBits.toBytes(t)
}
