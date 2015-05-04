package strd.util

import com.google.common.cache.{Cache, CacheBuilder}
import java.util.concurrent.{Callable, TimeUnit}
import net.sf.uadetector.service.UADetectorServiceFactory
import net.sf.uadetector.{ReadableUserAgent, UserAgentStringParser}
import com.twitter.ostrich.admin.Service

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 9/4/13
 * Time: 5:21 PM
 */
class CachedUserAgentsParser extends UserAgentStringParser with Service {

  val parser = UADetectorServiceFactory.getResourceModuleParser

  val cache: Cache[String, ReadableUserAgent] = CacheBuilder.newBuilder()
    .maximumSize(100000)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .expireAfterWrite(5, TimeUnit.HOURS)
    .build()

  def getDataVersion = parser.getDataVersion

  def parse(userAgent: String) = {
    cache.get(userAgent, new Callable[ReadableUserAgent] {
      def call() = parser.parse(userAgent)
    })
  }


  def start() {}

  def shutdown() {
    parser.shutdown()
  }
}
