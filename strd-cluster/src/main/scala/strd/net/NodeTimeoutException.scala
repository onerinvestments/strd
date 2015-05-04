package strd.net

import strd.cluster.proto.StrdCluster.NodeMeta
import java.util.concurrent.TimeoutException

/**
 *
 * User: light
 * Date: 06/05/14
 * Time: 12:52
 */
case class NodeTimeoutException(node : NodeMeta, trace : String = "") extends TimeoutException("Node timeout: " + node.getHostString +" as " + node.getNodeId + " timed out " + trace)
