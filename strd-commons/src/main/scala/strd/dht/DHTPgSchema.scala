package strd.dht

import org.squeryl.{PrimitiveTypeMode, Schema}


/**
 *
 * User: light
 * Date: 9/29/13
 * Time: 2:52 AM
 */
object DhtDialect extends PrimitiveTypeMode
import DhtDialect._
object DHTPgSchema extends Schema {

  val tokens = table[DHTToken]("dht_tokens")


  on(tokens)(t => declare(
    columns(t.key, t.cluster, t.version) are unique
  ))


}

class DHTToken( val key     : String,
                val node    : Int,
                val cluster : String,
                val version : Int = 0)

