package strd.dht

import strd.util._

/**
 *
 * User: light
 * Date: 08/11/13
 * Time: 13:40
 */


class DHTConfParams extends ConfigParamsDeclaration {
  val NODES_COUNT              = IntegerParam      ( "dht.nodes.count" )
  val ENTRIES_CACHE_SIZE       = BytesIntervalParam( "dht.store.entriesCacheSize",  defaultValue = "32mb" )
  val HEADERS_CACHE_SIZE       = BytesIntervalParam( "dht.store.headersCacheSize",  defaultValue = "128mb" )
  val BUCKET_SIZE              = BytesIntervalParam( "dht.store.bucketSize",        defaultValue = "1mb" )
  val STORE_FILE_SIZE          = BytesIntervalParam( "dht.store.storeFileSize",     defaultValue = "256mb" )
  val DHT_NODE_PORT            = IntegerParam      ( "dht.node.port",               defaultValue = 4660,      maximal = 64000, minimal = 1024 )
  val FLUSH_INTERVAL           = TimeIntervalParam ( "dht.store.flushInterval",     defaultValue = "10s" )
  val KV_STORE_ENV             = StringParam       ( "dht.store.env",               defaultValue = "/mnt/dht-node" )
  val COMPACT_PERCENT          = IntegerParam      ( "dht.node.compact.percent",    defaultValue = 10,        maximal = 99, minimal = 0 )
  val DHT_REPLICA_CONT         = IntegerParam      ( "dht_replica_count",           defaultValue = 3,         maximal = 99, minimal = 1 )
  val DHT_R                    = IntegerParam      ( "dht_r",                       defaultValue = 2,         maximal = 99, minimal = 1 )
  val DHT_W                    = IntegerParam      ( "dht_w",                       defaultValue = 3,         maximal = 99, minimal = 1 )
  val ENV_HOME                 = StringParam       ( "datanode.env", "/mnt" )
  val ENV3_HOME                = StringParam       ( "dhtnode.env",  "/mnt" )

}

object DHTConf extends DHTConfParams { }

