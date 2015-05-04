package strd.client

import akka.actor.{ActorRef, Actor}
import concurrent.duration._
import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.ExecutionContext
import strd.cluster._
import org.slf4j.LoggerFactory
import strd.cluster.RequestFailed
import strd.cluster.PackageSaved

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/13/13
 * Time: 5:17 PM
 */
class TableSender(table: Int, clientDispatcher: ActorRef)(implicit val executionContext: ExecutionContext) extends Actor {

  val pkgSize = 100
  val log = LoggerFactory.getLogger(getClass)
  val buffer = new mutable.ArrayBuffer[Any](pkgSize)
  var lastSendTime = Platform.currentTime
  var packages = 0L

  val sendInterval = 1.seconds      // TODO add property

  def receive = {


    case r: ShutdownReq => {
      scheduledTask.cancel()
      sendPackage()
    }

    case ScheduledSend => {
      if (lastSendTime + sendInterval.toMillis / 2 < Platform.currentTime) {
        sendPackage()
      }

    }

    case r: RequestFailed => {
      log.error(s"package failed $r")
    }

    case r: PackageSaved => {
      packages -= 1
    }

    case events: Seq[_] => {
      buffer ++= events
      if (buffer.size >= pkgSize) sendPackage()
    }

    case event => {
      buffer += event
      if (buffer.size >= pkgSize) sendPackage()
    }

  }

  def sendPackage() {
    if (!buffer.isEmpty) {
      ???

/*
      packages += 1
      clientDispatcher ! StrdPackage(MonotonicallyIncreaseGenerator.nextId(), table, buffer.toIndexedSeq, 10.seconds.toMillis)
      buffer.clear()
      lastSendTime = Platform.currentTime
*/
    }
  }

  private val scheduledTask = context.system.scheduler.schedule(sendInterval, sendInterval, self, ScheduledSend)

}

object ScheduledSend
