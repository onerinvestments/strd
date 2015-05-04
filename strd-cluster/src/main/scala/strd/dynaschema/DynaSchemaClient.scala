/*
package strd.dynaschema

import strd.util.{StrdNodeConf, ClusterConfigs}
import org.apache.curator.framework.CuratorFramework
import java.io.File
import org.apache.commons.lang.StringUtils
import com.twitter.ostrich.admin.Service
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import strd.net.{StrdProtos, ZkPathWithPurge}
import strd.cluster.proto.StrdCluster.DynaSchemaMeta
import strd.dht.BIN
import java.util.Date
import org.slf4j.LoggerFactory
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.net.{URL, URLClassLoader}
import java.util.concurrent.atomic.AtomicBoolean

/**
 *
 * User: light
 * Date: 16/04/14
 * Time: 17:25
 */

class DynaSchemaClient(implicit val bindingModule : BindingModule) extends Service with Injectable {

  val log = LoggerFactory.getLogger(getClass)
  implicit val conf = inject[ClusterConfigs]
  val curator       = inject[CuratorFramework]

  val storeName =  conf.nodeServiceId +"_" + conf.nodeId.get

  val storeDir = new File( StringUtils.replace( StrdNodeConf(_.DYNASCHEMA_STORE_DIR), "${STORE_NAME}", storeName) )

  val template = StrdNodeConf(_.DYNASCHEMA_DOWNLOAD_COMMAND)

  if (! storeDir.exists()) {
    if (! storeDir.mkdirs() ) {
      throw new IllegalStateException("Can not create store dir")
    }
  }

  val zkListener = new ZkPathWithPurge[DynaSchemaMeta, DynaSchemaClasses](curator, ZkSchema.zkPrefix) {
    override def onUpdated(x: DynaSchemaMeta, path: String, entry: DynaSchemaClasses) = {
      log.info(s"Update schema version:${x.getVersion} user:${x.getUser} time:${new Date(x.getTimestamp).toLocaleString}")
      loadJar( name(path), x, Some(entry) )
      //Du(x, file)
    }

    override def onAdded(x: DynaSchemaMeta, path: String) = {
      log.info(s"Append schema version:${x.getVersion} user:${x.getUser} time:${new Date(x.getTimestamp).toLocaleString}")
      loadJar( name(path), x, None )
    }

    override def serialize(msg: DynaSchemaMeta) = msg.toByteArray

    override def deserialize(bytes: BIN) = DynaSchemaMeta.parseFrom( bytes )
  }



  def loadJar(schemaType : String, meta : DynaSchemaMeta, old : Option[DynaSchemaClasses]) : DynaSchemaClasses = {

    val fileName = schemaType + "_" + meta.getVersion +".jar"
    val jarFile = new File( storeDir, fileName)
    if ( jarFile.exists() ) {
      log.info("schema jar :" + fileName + " present")
      old.getOrElse{ loadSchema(jarFile, meta, schemaType, old) }
    } else {
      val fullName = jarFile.getName

      val cmd = StringUtils.replace( StringUtils.replace(
        StringUtils.replace( template, "${DEST_FILE}", fullName),
        "${SCHEMA_TYPE}",schemaType),
        "${SCHEMA_VERSION}", meta.getVersion ).split("\\s").toSeq

      log.info("Loading jar: " + fileName +"  cmd:" + cmd)

      import scala.sys.process._

      val result = cmd ! ProcessLogger( x=> log.debug(x) )
      println("Rsync Exit code: " + result)

      if (result != 0) {
        throw new RuntimeException("Error while downloading schema")
      }
      //old.map(oldClasses =>  oldClasses.cleanUp() )
      loadSchema(jarFile, meta, schemaType, old)
    }
  }

  def loadSchema(jarFile : File,
                 meta : DynaSchemaMeta,
                 schemaType : String,
                 old : Option[DynaSchemaClasses] ) : DynaSchemaClasses = {
    val cl = new URLClassLoader( Array( new URL("file:///" +  jarFile.getName) ), Thread.currentThread().getContextClassLoader )

    val success = new AtomicBoolean(false)
    if (meta.hasProtoClass) {
      val updater = new Thread(new Runnable() {
        override def run() = {
          println("Invoke updater under new classLoader :" + Thread.currentThread().getName)
          // test proto class
          val cl = Thread.currentThread().getContextClassLoader.loadClass( meta.getProtoClass )
          StrdProtos.build(cl)
          // test proto class
          success.set(true)
        }
      }, "threadUpdater")

      updater.setContextClassLoader( cl )
      updater.start()
      updater.join()

    } else { success.set(true) }

    if (! success.get) {
      throw new RuntimeException("Updater failed")
    }
    val newCl = DynaSchemaClasses( meta, schemaType,  jarFile, cl)
    old.map( c => {
      newCl.setListeners( c )
      c.updateClassLoader( cl )
    })

    newCl

/*
    log.info( "Set classLoader :" + cl )
    val oldClassLoader = this.currentClassLoader.getAndSet( cl )
    val newPool = createThreadPool
    log.info( "Resubmit threadPool" )
    Option(this.currentPool.getAndSet(newPool)).foreach(oldPool => {
      val listRunnable = oldPool.shutdownNow()
      listRunnable.asScala.foreach(r => newPool.execute(r))
    })

    oldClassLoader.clear()
*/

  }

  def registerAndGetSchema(schemaName : String, executor: DynaSchemaListener) :DynaSchemaClasses = {
    val oldState = zkListener.state.get()
    val schema = oldState.seq.find(_.schemaName == schemaName).getOrElse{throw new RuntimeException("no schema found:" + schemaName)}
    schema.addListener(executor)
    schema
  }

  override def shutdown() = {
    zkListener.shutdown()
  }

  override def start() = {
    zkListener.start()
  }
}

trait DynaSchemaListener {
  def updateClassLoader( cl : ClassLoader )
}

case class DynaSchemaClasses( meta : DynaSchemaMeta,
                              schemaName : String,
                              jarFile : File,
                              cl : ClassLoader) {

/*
  var listeners : Seq[DynaSchemaListener] = Nil

  def addListener( l : DynaSchemaListener ) {
    synchronized {
      listeners = listeners :+ l
    }
  }

  def updateClassLoader(loader: URLClassLoader)  {
    synchronized{
      listeners.foreach(_.updateClassLoader(loader))
      listeners = Nil
    }
  }

  def setListeners(classes: DynaSchemaClasses){
    synchronized{
      listeners = classes.listeners
    }
  }
*/
}*/
