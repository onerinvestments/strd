package strd.common


import org.apache.curator.utils.ZookeeperFactory
import org.apache.zookeeper.{ClientCnxn, ZooKeeper, Watcher}
import org.slf4j.LoggerFactory

/**
 *
 * User: light
 * Date: 23/03/15
 * Time: 14:08
 */

class ExZookeeperFactory extends ZookeeperFactory{
  val log = LoggerFactory.getLogger(getClass)

  override def newZooKeeper(connectString: String, sessionTimeout: Int, watcher: Watcher, canBeReadOnly: Boolean) = {
    val zk = new ZooKeeper( connectString, sessionTimeout, watcher, canBeReadOnly );
    val f1 = zk.getClass.getDeclaredField("cnxn")
    f1.setAccessible(true)
    val cnxn = f1.get(zk).asInstanceOf[ClientCnxn]

    val eventThreadF = cnxn.getClass.getDeclaredField("eventThread")
    eventThreadF.setAccessible(true)
    val sendThreadF = cnxn.getClass.getDeclaredField("eventThread")
    sendThreadF.setAccessible(true)

    val eventThread = eventThreadF.get( cnxn ).asInstanceOf[ Thread ]
    eventThread.setPriority(Thread.NORM_PRIORITY + 1)

    val sendThread = sendThreadF.get( cnxn ).asInstanceOf[ Thread ]

    sendThread.setPriority(Thread.NORM_PRIORITY + 1)
    log.info("Create zookeeper using ExtFactory: ThreadPriority: " + sendThread.getPriority + " " + sendThread.getState)

    zk
  }
}
