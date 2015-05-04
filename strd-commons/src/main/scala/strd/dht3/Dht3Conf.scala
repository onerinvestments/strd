package strd.dht3

import strd.util._
import strd.util.{IntegerParam, StringParam, ConfigParamsDeclaration}

/**
 *
 * User: light
 * Date: 25/04/14
 * Time: 13:02
 */
class Dht3ConfParams extends ConfigParamsDeclaration {
  val NODES_COUNT             = IntegerParam     ( "dht.nodes.count" )
  val DHT_NODE_PORT           = IntegerParam     ( "dht.node.port",               defaultValue = 4659,      maximal = 64000, minimal = 1024 )
  val ENV_HOME                = StringParam      ( "dht.env",                     defaultValue = "/mnt" )
  val DHT_REPLICA_CONT        = IntegerParam     ( "dht.replica_count",           defaultValue = 1,         maximal = 99, minimal = 1 )
  val DHT_R                   = IntegerParam     ( "dht.r",                       defaultValue = 1,         maximal = 99, minimal = 1 )
  val DHT_W                   = IntegerParam     ( "dht.w",                       defaultValue = 1,         maximal = 99, minimal = 1 )
  // Optional: dht.nodes.count
}

object Dht3Conf extends Dht3ConfParams {}
