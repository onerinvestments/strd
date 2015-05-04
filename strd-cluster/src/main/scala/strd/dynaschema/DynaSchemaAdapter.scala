package strd.dynaschema

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.commons.lang.StringUtils
import org.apache.curator.framework.CuratorFramework
import strd.cluster.proto.StrdCluster.DynaSchemaMeta
import strd.dht._
import strd.net.{StrdProtos, ZkPathWithPurge}
import strd.util._

import scala.collection.convert.wrapAsScala._

/**
 *
 * User: light
 * Date: 16/04/14
 * Time: 19:27
 */

case class ValueAdapter[V](value : V, ds : DynaSchemaClasses) {
  override def equals(obj: scala.Any) = {
    obj.asInstanceOf[ValueAdapter[V]].ds.meta == ds.meta
  }
}

abstract class DynaSchemaAdapter[V]( config     : ClusterConfigs,
                                     curator    : CuratorFramework,
                                     zkPath     : String)
  extends ZkPathWithPurge[DynaSchemaMeta, ValueAdapter[V]](curator, zkPath) {

  implicit val conf = config

  val storeName = conf.nodeServiceId +"_" + conf.nodeId.get
  val storeDir = new File( StringUtils.replace( StrdNodeConf(_.DYNASCHEMA_STORE_DIR), "${STORE_NAME}", storeName) )
  val template = StrdNodeConf(_.DYNASCHEMA_DOWNLOAD_COMMAND)

  if (! storeDir.exists()) {
    if (! storeDir.mkdirs() ) {
      throw new IllegalStateException("Can not create store dir")
    }
  }

  override def onUpdated(x: DynaSchemaMeta, path: String, entry: ValueAdapter[V]) = {
    log.info("Updating DynaSchema:" + s"${x.getSchemaName}  proto:${x.getProtoClassList.mkString(", ")}  version:${x.getVersion} user:${x.getUser} time:${DateUtils.localeFormat(x.getTimestamp)}")

    val ds : DynaSchemaClasses = loadJar(  x, Some(entry.ds))
    handleUpdated( ds, entry).fold {
      log.error(s"!!!Can not update DynaSchema to version: ${x.getVersion}. Use old schema version: ${entry.ds.meta.getVersion}")
      ValueAdapter(entry.value, ds)
    } { v =>
      ValueAdapter(v, ds)
    }
  }

  override def onAdded(x: DynaSchemaMeta, path: String) = {
    log.info("Adding DynaSchema:" + s"${x.getSchemaName}  proto:${x.getProtoClassList.mkString(", ")}  version:${x.getVersion} user:${x.getUser} time:${DateUtils.localeFormat(x.getTimestamp)}")

    val ds : DynaSchemaClasses = loadJar( x, None )
    ValueAdapter( handleAdded( ds ), ds)
  }



  def handleAdded( ds : DynaSchemaClasses ) : V
  def handleUpdated( ds : DynaSchemaClasses, old : ValueAdapter[V] ) : Option[V]

  override def serialize(msg: DynaSchemaMeta) = msg.toByteArray

  override def deserialize(bytes: BIN) = DynaSchemaMeta.parseFrom( bytes )


  def loadJar( meta : DynaSchemaMeta, old : Option[DynaSchemaClasses]) : DynaSchemaClasses = {
    val schemaType  = meta.getSchemaName

    val fileName = schemaType + "_" + meta.getVersion +".jar"
    val jarFile = new File( storeDir, fileName)

/*
    if ( jarFile.exists() ) {
      log.info("schema jar :" + fileName + " present")
      old.getOrElse{ loadSchema(jarFile, meta) }
    } else {
*/
      val fullName = jarFile.getPath

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

      loadSchema(jarFile, meta)
//    }
  }

  def loadSchema(jarFile : File,
                 meta : DynaSchemaMeta
                 ) : DynaSchemaClasses = {
    val schemaType  = meta.getSchemaName
    val cl = new URLClassLoader(Array(new URL("file:///" + jarFile.getPath)), Thread.currentThread().getContextClassLoader)

    val success = new AtomicBoolean(false)
    if (meta.getProtoClassCount > 0) {
      val updater = new Thread(new Runnable() {
        override def run() = {
          println("Invoke updater under new classLoader :" + Thread.currentThread().getName)
          try {
            // test proto class
            meta.getProtoClassList.foreach { proto =>
              val cl = Thread.currentThread().getContextClassLoader.loadClass(proto)
              StrdProtos.build(schemaType, cl)
            }
            // test proto class
            success.set(true)
          } catch {
            case x:Exception =>
              log.error("Updater failed",x)
              success.set(false)
          }
        }
      }, "threadUpdater")

      updater.setContextClassLoader(cl)
      updater.start()
      updater.join()

    } else {
      success.set(true)
    }

    if (!success.get) {
      throw new RuntimeException("Updater failed")
    }
    val newCl = DynaSchemaClasses(meta, jarFile, cl)
/*
    old.map(c => {
      newCl.setListeners(c)
      c.updateClassLoader(cl)
    })
*/

    newCl
  }
}

case class DynaSchemaClasses( meta : DynaSchemaMeta,
                              jarFile : File,
                              cl : ClassLoader)
