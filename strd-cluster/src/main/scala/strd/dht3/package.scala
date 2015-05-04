package strd

import strd.dht._
import lmbrd.zn.util.PrimitiveBits

/**
 *
 * User: light
 * Date: 20/04/14
 * Time: 21:14
 */
package object dht3 {
  implicit def resolveTable[X, S <: DhtServerTable, A <: TableActivator[S]](d: DhtTableDescriptor[X,S,A])(implicit env: DhtClientEnvironement): X = {
    env.getClient(d)
  }

  def reverseLong( l : Long) = {
    Long.MaxValue - l
  }

  //  type DHTPROTO =  StrdProto[DhtSchemaRequest,DhtSchemaResponse]


}
