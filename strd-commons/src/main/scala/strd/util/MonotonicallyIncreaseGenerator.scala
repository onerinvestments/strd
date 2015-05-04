package strd.util

import scala.util.Random
import org.slf4j.LoggerFactory

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 7/31/13
 * Time: 4:15 PM
 */
class MonotonicallyIncreaseGenerator(val workerId  : Int) {
  import GeneratorConsts._

  val twepoch = 1349035200000L
//    1288834974657L
  val log = LoggerFactory.getLogger(getClass)


  var lastTimestamp = -1L
  var sequence: Long = 0L



  if (workerId > maxWorkerId || workerId < 0) {
    throw new IllegalStateException("can not generate workerId: " + workerId)
  }


  def nextId(): Long = synchronized {
    nextId(timeGen())
  }

  def nextId(time: Long): Long = synchronized {
    var timestamp = time
    if (timestamp < lastTimestamp) {
      log.error("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp)

      throw new IllegalStateException("Clock moved backwards.  Refusing to generate id for %d milliseconds".format(
        lastTimestamp - timestamp))
    }

    if (lastTimestamp == timestamp) {
      sequence = (sequence + 1) & sequenceMask
      if (sequence == 0) {
        timestamp = tilNextMillis(lastTimestamp)
      }
    } else {
      sequence = 0
    }

    lastTimestamp = timestamp

    ((timestamp - twepoch) << timestampLeftShift) |
      (workerId << workerIdShift) |
      sequence
  }

  def fromTimestamp(timestamp : Long) = {
    ((timestamp - twepoch) << timestampLeftShift) |
      (workerId << workerIdShift) |
      sequence
  }

  protected def tilNextMillis(lastTimestamp: Long): Long = {
    var timestamp = timeGen()
    while (timestamp <= lastTimestamp) {
      timestamp = timeGen()
    }
    timestamp
  }

  def fetchDate(id : Long) = {
    (id >> timestampLeftShift) + twepoch
  }

  def minIdForDate(date: Long) : Long = {
    (date - twepoch) << timestampLeftShift
  }

  protected def timeGen(): Long = System.currentTimeMillis()

}

object GeneratorConsts {
  val workerIdBits = 10L
  val maxWorkerId = -1L ^ (-1L << workerIdBits)
  val sequenceBits = 12L

  val workerIdShift = sequenceBits
  val timestampLeftShift = sequenceBits + workerIdBits
  val sequenceMask = -1L ^ (-1L << sequenceBits)

  val tsMask = -1L ^ (1L << timestampLeftShift)

}

object MonotonicallyIncreaseGenerator {

  val instance = new MonotonicallyIncreaseGenerator( Random.nextInt(GeneratorConsts.maxWorkerId.toInt) )
  def nextId(): Long = instance.nextId()
  def nextId(time: Long): Long = instance.nextId(time)

  def fetchDate(id : Long) = instance.fetchDate(id)

  def minIdForDate(date: Long) = instance.minIdForDate(date)

}

