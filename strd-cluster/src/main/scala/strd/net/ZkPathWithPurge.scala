package strd.net

import strd.dht.BIN
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.CuratorFramework
import java.util.{Timer, TimerTask}
import strd.cluster.proto.StrdCluster.NodeMeta
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import java.util.concurrent.Semaphore


/**
 *
 * User: light
 * Date: 16/04/14
 * Time: 17:30
 */

abstract class ZkPathWithPurge[X, S](curator : CuratorFramework, path : String) {

  case class PathState(entries : Map[String, S]) {
    lazy val seq = entries.values.toSeq
  }

  def deserialize(bytes: BIN): X

  def serialize(msg: X): BIN

  def onAdded(msg: X, path: String) : S

  def onUpdated(msg: X, path: String, entry : S ) : S

  def onRemoved(entry : S, path: String) = {}

  def onShutdown(entry : S, path: String) = {}

  def afterStateChanged(state : Seq[S]) = {}

  val state     = new AtomicReference[PathState]( PathState(Map.empty) )

  val zk = new PathChildrenCache(curator, path, true)
  val log = LoggerFactory.getLogger(getClass)

  if (curator.checkExists().forPath(path) == null) {
    log.info("Create initial path: " + path)
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
  }

  val shutdownFlag = new AtomicBoolean()

  val timer = new Timer()

  val firstPopulatedFlag = new Semaphore(0)

  def submit(f : => Unit)  {

    timer.schedule(new TimerTask() {
      override def run() = {
        try {
          if (!shutdownFlag.get()) {
            f
          }
        } catch {
          case x: Exception => log.error("Unexpected exception", x)
        }
      }
    }, 0)

  }

  def name(path : String) = path.substring( path.lastIndexOf("\\") )


  def start() {
    zk.getListenable.addListener( new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) = {
        if (! shutdownFlag.get()) {
          event.getType match {
            case PathChildrenCacheEvent.Type.CHILD_ADDED =>
              submit { appendEntry( name(event.getData.getPath), deserialize(event.getData.getData) ) }
            case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
              submit { updateEntry( name(event.getData.getPath), deserialize(event.getData.getData)) }
            case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
              log.debug("ZkEvent remove entry: " + event.getData.getPath)
              submit { removeEntry( name(event.getData.getPath) ) }

            case x => log.warn("Unexpected zk event: " + x)
          }
        }
      }
    })


    timer.scheduleAtFixedRate( new TimerTask(){
      override def run() = {
        try {
          if (! shutdownFlag.get()) {
            purgeZk()
          }

          if (firstPopulatedFlag.drainPermits() == 0)
            firstPopulatedFlag.release(1)

        } catch {
          case x:Exception => log.error("Failed zk purge", x)
        }
      }

    }, 1500, 1000 )

    log.debug("Await first purge...")
    firstPopulatedFlag.acquire()
  }

  def shutdown() = {
    val old = state.getAndSet(PathState(Map.empty))

    try {
      zk.close()
    } catch {
      case x: Exception =>
    }

    log.debug("Stopping zkPath " + path)
    shutdownFlag.set(true)
    timer.cancel()

    old.entries.foreach(n => invokeListener{ onShutdown( n._2 , n._1)})
    invokeListener{ afterStateChanged(Nil) }
  }

  def metaForValue(s:S) : X

  def purgeZk() {
    val iter = curator.getChildren.forPath( path ).iterator()

    val map = collection.mutable.Map[String, X]()

    while (iter.hasNext) {
      val next = iter.next()
      val meta = deserialize( curator.getData.forPath(path +"/" + next) )
      map += (next -> meta)
    }

    val s = state.get()
    map.foreach{ case (key,value) =>
      s.entries.get( key ).map{ old => if (metaForValue(old) != value) { updateEntry(key, value) } }.getOrElse {
        log.debug("Add entry by purge")
        appendEntry( key, value )
      }
    }

    s.entries.foreach{ case (key, value) =>
      map.get( key ).getOrElse{
        log.debug("Remove entry by purge")
        removeEntry( key  )
      }
    }
  }

  final def appendEntry(path : String, meta : X) {
    val s = state.get()
    if (!s.entries.contains(path)) {
      log.debug("Adding entry: " + meta)
      val ss = onAdded(meta, path)
      val s2 = s.copy(s.entries + (path -> ss))
      state.set(s2)
      invokeListener{ afterStateChanged(s2.seq) }
    }

  }

  final def updateEntry(path : String, meta : X) {
    val s = state.get()

    s.entries.get(path).map(x => {
      val ss = onUpdated( meta, path, x )
      val s2 = s.copy(s.entries + (path -> ss))
      state.set(s2)
      invokeListener{ afterStateChanged(s2.seq) }
    }).getOrElse {
      log.warn("No entry for update :" + meta)
    }
  }

  def removeEntryImpl( path : String ) {
    submit {
      val s = state.get()
      s.entries.get( path ).map( x => {
        val s2 = s.copy(entries = s.entries - path)
        state.set(s2)
      })
    }
  }

  final def removeEntry(path : String) {
    val s = state.get()
    s.entries.get( path ).map( x => {
      val s2 = s.copy(entries = s.entries - path )
      state.set(s2)
      invokeListener{ onRemoved(x, path ) }
      invokeListener{ afterStateChanged(s2.seq) }
    }).getOrElse{
      log.warn("No entry for remove :" + path)
    }
  }

  private def invokeListener( f: => Unit) {
    try { f } catch { case ignored : Exception => log.error("Listener failed", ignored) }
  }


}
