package strd.util

/**
 *
 * User: lembrd
 * Date: 02/05/15
 * Time: 15:49
 */

object ClusterNodeInfo extends App {

  val conf = new Object with StrdService
    with ClusterStrdConf {

    override val registerNodeInCluster = false
    val clusterNodeName = "test-node"

  }

  ServerStarter(conf).start()
}
