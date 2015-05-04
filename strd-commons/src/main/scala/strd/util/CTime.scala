package strd.util

/**
 *
 * User: light
 * Date: 12/04/14
 * Time: 17:04
 */

object CTime {

/*
  val timer = new Thread("time") {
    override def run() = {
      while (! Thread.currentThread().isInterrupted) {
        cctime = System.currentTimeMillis()
//        ctime.set(System.currentTimeMillis())
        Thread.sleep(1)
      }
    }
  }

  timer.setDaemon(true)
  @volatile var cctime = System.currentTimeMillis()
  val ctime = new AtomicLong( System.currentTimeMillis() )
  timer.start()
*/

  @inline
  def now = System.currentTimeMillis()
    //cctime
    //System.currentTimeMillis()
    //ctime.get()
    //

}
