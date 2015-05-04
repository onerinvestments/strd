package strd

import org.slf4j.Logger

import scala.concurrent.Promise

/**
 *
 * User: light
 * Date: 9/4/13
 * Time: 4:57 PM
 */

object SafeUtil {
  // promise exception wrapper
  def pxw[X,A](p:Promise[X])(handler: => A) = {
    try {
      handler
    } catch {
      case x: Exception =>
        p.tryFailure(x)
        throw x
    }
  }

  def safeLogExec[X](f: => X)(implicit log: Logger): Option[X] = {
    try {
      Some(f)
    } catch {
      case t: Exception =>
        log.warn("Safe method failed", t)
        None
    }
  }

  def safeLogExec(f: => Unit)(implicit log: Logger) {
    try {
      f
    } catch {
      case e: Exception =>
        log.warn("Safe method failed", e)
    }
  }

  def exceptionRetry[X]( message : String, retryCount : Int = 3, sleep : Int = 100 )( f : => X)(implicit log : Logger) : X = {
    var errorCounter: Int = retryCount
    var result :Option[X] = None

    while (errorCounter > 0) {
      try {
        result = Some( f )
        errorCounter = 0
      } catch {
        case x : Exception =>
          errorCounter -= 1

          log.warn(s"$message, attemptsLeft:$errorCounter", x)

          if ( errorCounter > 0 ) {
            Thread.sleep( Math.max(100, sleep * (retryCount - errorCounter) ) )
          } else {
            throw x
          }
      }
    }
    result.get
  }

}
