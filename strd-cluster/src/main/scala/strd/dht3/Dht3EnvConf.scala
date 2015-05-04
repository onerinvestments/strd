package strd.dht3

import strd.util._

/**
 *
 * User: light
 * Date: 28/04/14
 * Time: 13:59
 */

trait Dht3EnvConf { self : StrdService
//                        with CuratorConfSimple
                        with SquerylSessionFactory
                        with ClusterStrdConf  =>

//  bind[DHTRegistry]           toSingle new DHTRegistry(ClusterConfigs.cluster.get + "_dht3")
  bind[DhtClient3]            toSingle new DhtClient3Impl("dht3") // same as DhtNode
  bind[DhtClientEnvironement] toSingle new DhtClientEnvironementImpl()

}
