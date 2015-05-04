package strd.concurrent

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

/**
 * @author Kirill chEbba Chebunin
 */
class MapState[K, V] {
  private val map = new ConcurrentHashMap[K, V]()

  def get(key: K) = Option(map.get(key))

  def size = map.size()

  def update(handler: (Updater) => Unit) {
    val updater = new Updater()
    handler(updater)

    val iter = map.entrySet().iterator()
    while(iter.hasNext) {
      val e = iter.next()
      if (!updater.newItems.contains(e.getKey) ) {
        iter.remove()
      }
    }
  }

  class Updater() {
    val newItems = new mutable.HashSet[K]()
    
    def update(key: K, value: V) {
      newItems += key
      map.put(key, value)
    }
  }
}
