package strd.cluster

import org.iq80.leveldb.DB
import strd.dht._
import lmbrd.zn.util.BytesHash

/**
 *
 * User: light
 * Date: 10/03/14
 * Time: 02:40
 */

object LevelDB {

  def list(db: DB, fromKey: BIN, toKey: BIN)(handler: ((BIN, BIN) => Boolean)) = {
    //val traceLevel =
    val iterator = db.iterator()
    var counter = 0
    try {
      iterator.seek(fromKey)

      var finished = false

      while (iterator.hasNext && !finished) {
        val next = iterator.peekNext()

        val keyBytes = next.getKey
        val valueBytes = next.getValue

        if (BytesHash.compareTo(keyBytes, toKey) > 0) {
          finished = true
        }

        if (! finished) {
          counter += 1

          finished = ! handler(keyBytes, valueBytes)
          iterator.next()
        }

      }
      counter
    } finally {
      iterator.close()
    }
  }

}
