package strd.dht3

import com.google.protobuf.Message
import scala.concurrent.Future


/**
 *
 * User: light
 * Date: 21/04/14
 * Time: 19:03
 */

case class DhtTableDescriptor[CLIENT, S <: DhtServerTable, A <: TableActivator[S]](
                                                  tableId : Int,
                                                   schema : DhtProto,
                                                clFactory : (DhtTableInit) => CLIENT,
                                               sr : Class[A] )(implicit m:Manifest[CLIENT]) {
}

case class DhtTableInit( tableId : Int, proto : DhtProto, client : Dht3ClientApi )

abstract class DhtDbSchema {
  implicit val dbSchema = this
  val tables = new collection.mutable.ArrayBuffer[DhtTableDescriptor[_,_,_]]()

  def SCHEMA_NAME    : String
  def SCHEMA_VERSION : String
  def schema         : DhtProto

  def descriptor[X, S <: DhtServerTable, A <: TableActivator[S] ]( tableId : Int )(factory : (DhtTableInit) => X)(implicit m1 : Manifest[X], m2 : Manifest[A]) : DhtTableDescriptor[X, S, A] = {
    val d = DhtTableDescriptor[X, S, A](tableId, schema, factory, m2.runtimeClass.asInstanceOf[Class[A]])
    tables += d
    d
  }

  def register( ds : DhtTableDescriptor[_,_,_]*) {
    tables.++=(ds)
  }
}

trait DhtClientEnvironement {
  def getClient[X]( d : DhtTableDescriptor[X,_,_] ) : X
}
