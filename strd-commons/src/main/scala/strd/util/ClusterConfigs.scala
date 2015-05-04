package strd.util

import java.io.{File, FileInputStream}
import java.net.{InetAddress, InetSocketAddress, NetworkInterface, Socket}
import java.sql.{Connection, DriverManager, PreparedStatement, Statement}
import java.util.Properties

import com.eaio.util.lang.Hex
import com.escalatesoft.subcut.inject.{BindingModule, Injectable, MutableBindingModule}
import com.mchange.v2.c3p0.ComboPooledDataSource
import io.netty.handler.codec.http.QueryStringDecoder
import org.slf4j.LoggerFactory
import strd.pg.SchemaConnectionCustomizer

import scala.collection.{mutable, _}
import scala.util.Random


/**
 *
 * User: light
 * Date: 8/13/13
 * Time: 2:37 PM
 */

object ClusterConfigs {

  lazy val shortHostName : String = {
    InetAddress.getLocalHost.getHostName.takeWhile(x => x != '.')
  }

  var cluster : Option[String]  = None
  val log = LoggerFactory.getLogger("cluster")

  val localOutgoingIp : String = {
    val socket = new Socket()
    socket.connect(new InetSocketAddress("google.com", 80))

    val localOutgoingIp = socket.getLocalAddress.getHostAddress
    socket.close()
    localOutgoingIp
  }


  val publicAddress : String = Option( System.getProperty("MY_IP") ).getOrElse(localOutgoingIp)

/*

  d1 = (id:"dn1", port:3187) on dev3
  d2 = "dn2"

  val (d1, d2, d3) = dev3.instances(3)

  val  datanodes = build( "strd-node" )
  datanodes on (d1, d2)


    //
    println("LOCAL OUTGOING IP: " + localOutgoingIp)
    //
    while (enums.hasMoreElements) {
      val e = enums.nextElement()

      if (e.isUp && !e.isLoopback && !e.isPointToPoint) {
        val addrs = e.getInetAddresses
        while (addrs.hasMoreElements) {
          val a = addrs.nextElement()

          if ( a.getAddress.length == 4) {  // IPv4
            buf += a.getHostAddress
          }

        }
      }
    }

    if (buf.size != 1) {
      println("Buf.size " + buf.mkString("\n"))
    }

    buf.headOption.getOrElse{
      throw new RuntimeException("Failed to fetch network interface")
    }
*/

  val macForRegistration : String = {
    val publicIface = InterfaceUtils.interfaces.find( e => e.isUp && !e.isLoopback && !e.isPointToPoint && e.addresses.exists(_.getHostAddress == localOutgoingIp))
    publicIface.map(x=>{
      log.info("Using interface: " + x.getDisplayName + " withMac :" + x.getHexMac + " for cluster registration")
      x.getHexMac
    }).getOrElse{ throw new IllegalStateException("can not fetch Interface Mac for IP: " + localOutgoingIp)}
  }

  log.info(
    s"""
      |
      |LOCAL_IP: $localOutgoingIp
      |PUBLIC_IP: $publicAddress
      |MAC: $macForRegistration
      |
    """.stripMargin)

}

class StrdNodeConfParams extends ConfigParamsDeclaration {
  val ZK_ADDR                     = StringParam ("zkAddr")
  val DYNASCHEMA_UPLOAD_COMMAND   = StringParam ("dynaschema.upload.command", Some("rsync -av ${SRC_FILE} dev.lembrd.com:/srv/dynaschema/${SCHEMA_TYPE}/"))

  val DYNASCHEMA_DOWNLOAD_COMMAND = StringParam ("dynaschema.download.command", Some("rsync -av dev.lembrd.com:/srv/dynaschema/${SCHEMA_TYPE}/${SCHEMA_TYPE}_${SCHEMA_VERSION}.jar ${DEST_FILE}"))
  val DYNASCHEMA_STORE_DIR        = StringParam ("dynaschema.store.dir", Some("/tmp/strd/${STORE_NAME}/dynaschema/"))

  val GRAPHITE_HOST               = StringParam ("graphite.host")
  val GRAPHITE_PORT               = IntegerParam("graphite.port", 2003)

  val CONNECTOR_TIME_DIFF         = IntegerParam("connector.timeDiff", defaultValue = 100)


  def OSTRICH_PORT(nodeName: String) = IntegerParam(nodeName + ".ostrich.http")


}

object StrdNodeConf extends StrdNodeConfParams {
}

trait ClusterStrdConf { this : StrdService =>
  def clusterNodeName : String
  def registerNodeInCluster : Boolean = true
  println("--> clusterStrdConf")

  implicit lazy val conf = new ClusterConfigs(clusterNodeName, registerNodeInCluster)

  bind[ClusterConfigs] toSingle(conf)
}

class ClusterConfigs(val _nodeServiceId : String, val register : Boolean = true)/*(implicit val bindingModule: BindingModule) extends Injectable*/  {
  CTime.now

  val nodeServiceId = _nodeServiceId.toLowerCase

  val pathToClusterId = "/etc/strd.cluster"

  val props = mutable.Map.empty[String, String]
  val nodes = mutable.Map.empty[Int, StaticNodeDescription]

  val log = LoggerFactory.getLogger(getClass)

  var nodeId  : Option[Int]     = None

  lazy val hostName = try { java.net.InetAddress.getLocalHost.getHostName } catch {case x:Exception => "n/a: " + nodeId.getOrElse("-") }

  val publicAddress  = ClusterConfigs.publicAddress


  def nodeForType( nodeType : String ) : Seq[StaticNodeDescription] = {
    nodes.values.filter( _.nodeType == nodeType).toSeq
  }

  def nodeForId( id : Int ) : Option[StaticNodeDescription] = {
    nodes.get(id)
  }


  def pooledDS( maxPoolSize     : Int = 3,
                userProp        : String = "conf.db.user",
                pwdProp         : String = "conf.db.pwd",
                urlProp         : String = "conf.db.url",
                initialPoolSize : Int = 1) = {

    val ds = new ComboPooledDataSource()

    val url = get(urlProp)

    ds.setDriverClass("org.postgresql.Driver")
    ds.setJdbcUrl( url )
    ds.setUser( get(userProp) )
    ds.setPassword( get(pwdProp) )

    ds.setInitialPoolSize(initialPoolSize)
    ds.setMinPoolSize(initialPoolSize)

    ds.setMaxPoolSize(maxPoolSize)
    ds.setMaxIdleTime(30)
    ds.setMaxIdleTimeExcessConnections(20)

    import scala.collection.JavaConverters._
    val params = new QueryStringDecoder(url).parameters().asScala

    val schema = params.get("schema").flatMap(_.asScala.headOption)

    schema.foreach { s =>
      SchemaConnectionCustomizer.schemas(ds.getDataSourceName) = s
      ds.setConnectionCustomizerClassName(classOf[SchemaConnectionCustomizer].getName)
    }

    log.debug("--> testing database: " + ds.getJdbcUrl)
    val con = ds.getConnection
    try {
      val st = con.createStatement()
      try {
        val rs = st.executeQuery("select now();")
        rs.next()
        println("PG:test:" + rs.getTime(1))
        rs.close()
      } finally {
        st.close()
      }
    } catch {
      case x:Exception=> log.error("Failed, " + ds.getUser + " : " + ds.getJdbcUrl, x)
      throw x
    } finally {
      con.close()
    }

    ds
  }

  @deprecated(message = {"deprecated"}, since = "2.0")
  def bind(name : String)(implicit module : MutableBindingModule) {
    try {
      val intValue = get(name).toInt
      module.bind[Int] idBy name toSingle intValue
    } catch {
      case x: Exception => module.bind[String] idBy name toSingle get(name)
    }
  }

//  @deprecated
  def get(name :String) : String = {
    props.getOrElse(name, {throw new IllegalArgumentException(s"Property $name is not found")})
  }

//  @deprecated
  def getInt(name :String) : Int = {
    get(name).toInt
  }

//  @deprecated
  def getOptInt( name : String) : Option[Int] = props.get(name).map( _.toInt)

  def selectNodes(con: Connection) {
    val st = con.prepareStatement("select node_id, node_type, addr  from cluster_nodes where cluster = ?")
    try {
      st.setString(1, ClusterConfigs.cluster.get)
      val q = st.executeQuery()
      try {
        while(q.next()) {
          val d = StaticNodeDescription( q.getInt(1), q.getString(2), q.getString(3), ClusterConfigs.cluster.get )
          nodes += ( d.nodeId -> d )
        }
      } finally {
        q.close()
      }
    } finally {
      st.close()
    }
  }


  def load() {
    val f = new File(pathToClusterId)

    if ( f.exists() ) {
      val properties = new Properties()
      properties.load( new FileInputStream(f))

      val enums = properties.propertyNames()

      while ( enums.hasMoreElements ) {
        val key = enums.nextElement()
        val value = properties.get(key)
        this.props += (key.toString -> value.toString)
      }

      props.get("conf.db.user").flatMap( user => props.get("conf.db.pwd").flatMap( pwd => props.get("conf.db.url").flatMap( url => props.get("conf.cluster").map( cl => {

        if (ClusterConfigs.cluster.isEmpty) {
          ClusterConfigs.cluster = Some(cl)
        }

        val cluster = ClusterConfigs.cluster.get

        log.debug(s"Fetching cluster conf using: $url @ $user in cluster $cluster")

        val con = DriverManager.getConnection(url, user, pwd)
        try {
          val st = con.prepareStatement("select node_id, addr from cluster_nodes where mac = ? and node_type = ? and cluster = ?")
          try {
            st.setString(1, ClusterConfigs.macForRegistration)
            st.setString(2, nodeServiceId)
            st.setString(3, cluster)

            val query = st.executeQuery()
            try {
              if ( query.next() ) {
                nodeId = Some( query.getInt(1) )
                if (query.getString(2) != publicAddress) {
                  log.debug(s"Update self address from ${query.getString(2)} to $publicAddress")
                  updateSelfAddr(con)
                }
              }
            } finally {
              query.close()
            }

            if (nodeId.isEmpty) {
              if (register) {
                val put = con.prepareStatement("insert into cluster_nodes (mac, node_type, cluster, addr) values (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)
                try {
                  put.setString(1, ClusterConfigs.macForRegistration)
                  put.setString(2, nodeServiceId)
                  put.setString(3, cluster)
                  put.setString(4, publicAddress)

                  val count  = put.executeUpdate()
                  if (count != 1) {
                    throw new RuntimeException("can not insert")
                  }

                  val keys = put.getGeneratedKeys
                  keys.next()
                  nodeId = Some( keys.getInt(1) )

                  log.debug("Node Record created for mac address")

                } finally {
                  put.close()
                }
              } else {
                nodeId = Some( 1000 + Random.nextInt(GeneratorConsts.maxWorkerId.toInt - 1000))
              }
            }
          }  finally {
            st.close()
          }
          log.info("MY NODE_ID is:" + nodeId)

          // select properties

          selectProperties(con)

          selectNodes(con)
          if (register) {
            if( ! nodeForType(nodeServiceId).exists(_.nodeId == nodeId.get) ) {
              throw new RuntimeException("should not happens")
            }

            if( ! nodeForId( nodeId.get).exists(_.nodeType == nodeServiceId) ) {
              throw new RuntimeException("should not happens")
            }
          }

        } finally {
          con.close()
        }
      })))).getOrElse{ throw new RuntimeException( "props not enough for reading cluster configs" ) }
    } else {
      log.warn(s"No properties: $pathToClusterId")
    }
  }


  def updateSelfAddr( con: Connection) {


    val up = con.prepareStatement("update cluster_nodes set addr = ? where node_id = ?")
    up.setString(1, publicAddress)
    up.setInt(2, nodeId.get)
    try {
      up.executeUpdate()
    } finally {
      up.close()
    }
  }

  def appendProps(statement: PreparedStatement) {
    val rs = statement.executeQuery()
    try {
      while (rs.next()) {
        val key = rs.getString(1)
        val value = rs.getString(2)

        props += (key -> value)
      }
    } finally {
      rs.close()
    }
  }

  def withinStatement(con:Connection, q:String)(functor : PreparedStatement => Unit) {
    val st = con.prepareStatement(q)
    try {
      functor(st)
      appendProps( st )
    }  finally {
      st.close()
    }
  }

  def selectProperties(con: Connection) {
    withinStatement(con, "select key, value from conf where node_type is null and cluster is null and node_id is null") { s =>
    }

    withinStatement(con, "select key, value from conf where node_type is null and cluster = ?") { s =>
      s.setString(1, ClusterConfigs.cluster.get)
    }

    withinStatement(con, "select key, value from conf where node_type = ? and cluster is null") { s =>
      s.setString(1, nodeServiceId)
    }

    withinStatement(con, "select key, value from conf where node_type = ? and cluster = ?") { s =>
      s.setString(1, nodeServiceId)
      s.setString(2, ClusterConfigs.cluster.get)
    }

    withinStatement(con, "select key, value from conf where node_id = ?") { s =>
      s.setInt(1, nodeId.get)
    }

  }

  load()

  val bindInterface : Option[IfaceInfo] = {
    props.get("cluster.bind_iface").flatMap( iface => {
      InterfaceUtils.findByName(iface).map( x => {
        log.debug("BindInterface: " + x.getName + " hex: " + x.getHexMac)
        Some(x)
      }).getOrElse{
        log.warn("BindInterface from props: " + iface + " is not found")
        None
      }

    })
  }

  val clusterBindAddress : String = {
    bindInterface.flatMap( info => info.addresses.map( a => a match {

      case inet :InetAddress if (! inet.isMulticastAddress) =>
        log.debug("Find bind address: " + inet.getHostAddress)
        Some( inet.getHostAddress )
      case x =>
        log.debug("Skip adress: " + x + " " + x.getHostAddress)
        None
    }).flatten.headOption).getOrElse(publicAddress)
  }

  private[this] val generatorInstance = new MonotonicallyIncreaseGenerator( nodeId.get )

  def nextId(): Long = generatorInstance.nextId()
  def nextId(time: Long): Long = generatorInstance.nextId(time)

  def fetchDate(id : Long) = generatorInstance.fetchDate(id)

  def minIdForDate(date: Long) = generatorInstance.minIdForDate(date)

}

case class StaticNodeDescription(nodeId : Int, nodeType : String, addr : String, cluster : String)
