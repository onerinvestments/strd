package strd.kafka

import strd.util._

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 20/10/14
 * Time: 14:33
 */
class KafkaConfParams extends ConfigParamsDeclaration {

  val KAFKA_BROKER_PORT           = IntegerParam("kafka.broker.port",       defaultValue = 9092,      maximal = 64000,  minimal = 1024  )
  val USE_KAFKA                   = IntegerParam("use.kafka",               defaultValue = 1,         minimal = 0,      maximal = 1     )
  val PARTITIONS_COUNT            = IntegerParam("kafka.partitions.count",  defaultValue = 20,        minimal = 1,      maximal = 100000)
  val REP_FACTOR                  = IntegerParam("kafka.rep.factor",        defaultValue = 2,         minimal = 1,      maximal = 10    )
  val THREADS_FOR_TOPIC           = IntegerParam("kafka.topic.threads",     defaultValue = 1,         minimal = 1,      maximal = 10    )

  def KAFKA_ZK_ADDRESS(implicit conf: ClusterConfigs) = s"${StrdNodeConf(_.ZK_ADDR)}$KAFKA_SUFFIX"
  val KAFKA_SUFFIX = s"/${ClusterConfigs.cluster.get}/kafka"
}

object KafkaConf extends KafkaConfParams{

}
