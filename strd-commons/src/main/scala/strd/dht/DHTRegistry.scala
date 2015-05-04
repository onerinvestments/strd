package strd.dht

import java.util.concurrent.atomic.AtomicReference

import org.slf4j.LoggerFactory
import strd.util.ClusterConfigs

object PGMode extends org.squeryl.PrimitiveTypeMode
import strd.dht.PGMode._
/**
 *
 * User: light
 * Date: 9/29/13
 * Time: 2:20 AM
 */

class DHTRegistry(cluster : String, conf : ClusterConfigs)  {

  val log       = LoggerFactory.getLogger(getClass)
  val nodeId    = conf.nodeId.get

  val state     = new AtomicReference[DHTRing]
  val oldState  = new AtomicReference[Option[DHTRing]]

  def start() {
    val tokens = loadTokens()
    tokens.size match {
      case 0 => throw new IllegalStateException("No tokens found")
      case size if size > 2 => throw new IllegalStateException(s"More than 2 ring versions: $size")
      case _ =>
        state.set(tokens(0))
        oldState.set(tokens.lift(1))
    }
  }

  def loadTokens() = {
      inTransaction {
        val tokens = from(DHTPgSchema.tokens)(t =>  where(t.cluster === cluster) select t).groupBy(_.version)
        log.debug("Ring tokens loaded for versions: " + tokens.keys)
        List(tokens.toSeq.sortBy(_._1).reverse.map(p => new DHTRing(p._2.toIndexedSeq)):_*)
      }
  }

  def ring = Option( state.get() ).getOrElse{ throw new IllegalStateException("Is not initialized") }
  def oldRing = Option( oldState.get() ).getOrElse{ throw new IllegalStateException("Is not initialized") }


  start()
}






