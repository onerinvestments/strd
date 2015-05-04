package strd.util

import java.util

import com.escalatesoft.subcut.inject.{BindingModule, MutableBindingModule}
import com.twitter.ostrich.admin.{Service, ServiceTracker}
import com.twitter.ostrich.stats.Stats
import org.slf4j.LoggerFactory
import strd.trace.PoolContext
import sun.misc.VM

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.sys.ShutdownHookThread

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 4/15/13
 * Time: 3:47 PM
 */
class ServerStarter(val bindings: BindingModule, ss : Option[StrdService] = None) {
  import scala.collection.JavaConverters._

  val log = LoggerFactory.getLogger( getClass )



  def start() {
    log.info("--> Start")
    val begin  = System.currentTimeMillis()

    //ServiceTracker.register(this)
    val toStart = new util.TreeMap[Int, Service]()
    try {
      bindings.bindings .foreach(binding => {
        val o = bindings.injectOptional(binding._1)
        o match {
          case Some(srv: Service) =>
            ss.map { s =>
              toStart.put(s._bindOrdering.get(ServiceKey(binding._1.m, binding._1.name)).get, srv)
            }.getOrElse {
              toStart.put(toStart.size(), srv)
            }

          case _ =>
        }
      })

      toStart.asScala.values.foreach{ srv => {
        log.debug(" -> start " + srv.getClass.getName)
        ServiceTracker.register(srv)
        srv.start()
      }}

    } catch {
      case x: Exception => {
        log.error("Starting failed", x)
        System.exit(-5)
      }
    }



    ShutdownHookThread(shutdown())

    val configs = bindings.injectOptional[ClusterConfigs](None)

    val message =
    s"""
      |===================================================
      |   Server Started in ${System.currentTimeMillis() - begin}ms
      |   MaxDirectMem : ${(VM.maxDirectMemory() / 1024d / 1024d).toInt} mb
      |   BindingProps :
      |""".stripMargin +
      bindings.bindings.filterNot( b => b._1.name.exists( n=> configs.exists( c => c.props.contains( n ))) ).map( b => bindings.injectOptional(b._1) match {
        case Some(s : String) =>
     f"     ${b._1.name.getOrElse("[noname]")}%-16s => '$s'\n"
        case Some(i : Int) =>
     f"     ${b._1.name.getOrElse("[noname]")}%-16s => $i (int)\n"

        case _ => ""
      }).mkString("") +
      configs.map( cfg =>
   s"""
      |   NodeType  : ${cfg.nodeServiceId}
      |   HostName  : ${ClusterConfigs.shortHostName}
      |   Mac       : ${ClusterConfigs.macForRegistration}
      |   IP_PUB    : ${ClusterConfigs.publicAddress}
      |   IP_INT    : ${cfg.clusterBindAddress}
      |   BindIface : ${cfg.bindInterface.map(iface => iface.getName).getOrElse("n/a")}
      |   NodeID    : ${cfg.nodeId.getOrElse("no")}
      |   OutAddr   : ${ClusterConfigs.localOutgoingIp}
      |   ClusterNodes:
      |""".stripMargin + cfg.nodes.values.map( node =>
     f"     ${node.nodeType}%-20s  | ${node.addr}%-16s  | ${node.nodeId}%-5d  | ${node.cluster}\n"
        ).mkString("") +
   s"""
      |   ClusterProps:
      |""".stripMargin + cfg.props.map( param =>
     f"     ${param._1}%-35s => ${if (param._1.contains("pwd") || param._1.contains("password")) "*****" else param._2}\n"
   ).mkString("") ).getOrElse("") +
   s"""
      |=======================(.)(.)======================
      |
    """.stripMargin

    log.info(message)

  }

  def shutdown() {
    ServiceTracker.synchronized {
      Stats.setLabel("Shutdown:", "quiesce...")
      log.info("--> Quiesce")

      ServiceTracker.peek.foreach { x=>
        log.debug("-> Quiesce: " + x.getClass.getName)
        try {
          x.quiesce()
        } catch {
          case x: Exception => log.warn("Failed: ", x)
        }
        log.debug("<- Quiesce: " + x.getClass.getName)
      }

      log.info("<-- Quiesce")
      Stats.setLabel("Shutdown:", "shutdown...")
      log.info("--> Shutdown")

      ServiceTracker.peek.foreach {
        x =>
          log.debug("-> stop: " + x.getClass.getName)
          try {
            x.shutdown()
          } catch {
            case e: Exception => log.warn("Service Stop Failed :" + x.getClass.getName, e)
          }
          log.debug("<- stop" + x.getClass.getName)
      }

      log.info("<-- Shutdown")
      Stats.setLabel("Shutdown:", "completed")
    }
  }

}

trait LateInitiable {

  def lateInit()

}

trait StrdService extends MutableBindingModule with LateInitiable{
  val _bindOrdering = new mutable.LinkedHashMap[ServiceKey, Int]()

  implicit val module : MutableBindingModule = this



  println("--> StrdService")

  bind[ExecutionContext] toSingle PoolContext.cachedExecutor(name = "default")



/*
  override def bind[T](implicit m: Manifest[T]) = {
    _bindOrdering += ( ServiceKey(m, None) -> _bindOrdering.size)
    super.bind
  }
*/
  override def bind[T <: Any](implicit m: scala.reflect.Manifest[T]) = {
    new Bind[T]() {
      override def toSingle[I <: T](function: => I) = {
        _bindOrdering += ( ServiceKey(m, name) -> _bindOrdering.size)
        super.toSingle(function)
      }
    }
  }

  def lateInit() {
    // nothing
  }
  //def init() {}

}

object ServerStarter{
  def apply( s : StrdService) : ServerStarter = {

    s match {
      case l : LateInitiable => l.lateInit()
      case _ =>
    }

    new ServerStarter( s.freeze().fixed, s )
  }
}

case class ServiceKey(m:Manifest[_], id : Option[String])