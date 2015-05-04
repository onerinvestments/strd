package strd.dht3

import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.ostrich.admin.Service
import java.util.concurrent.ConcurrentHashMap

/**
 *
 * User: light
 * Date: 22/04/14
 * Time: 12:56
 */

class DhtClientEnvironementImpl(implicit val bindingModule : BindingModule) extends Injectable with Service with DhtClientEnvironement {
  val client = inject[DhtClient3]
  val map = new ConcurrentHashMap[Int, Any]()

  override def getClient[X](d: DhtTableDescriptor[X,_, _]) = {
    var t = map.get(d.tableId)
    if (t == null) {
      map.synchronized{
        t = map.get(d.tableId)
        if (t == null) {
          t = d.clFactory( DhtTableInit(d.tableId, d.schema, client.dhtConnector) )
          map.put(d.tableId, t)
        }
      }
    }
    t.asInstanceOf[X]
  }

  override def shutdown() = {

  }

  override def start() = {

  }


}
