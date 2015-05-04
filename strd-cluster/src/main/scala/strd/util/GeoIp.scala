package strd.util

import java.net.InetAddress

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.maxmind.geoip2.GeoIp2Provider
import strd.commons.proto.StrdCommonsProto.GeoInfo

import scala.collection.convert.decorateAsScala._

/**
 *
 */
class GeoIp(implicit val bindingModule: BindingModule) extends Injectable {
  val geoip = inject[GeoIp2Provider]

  def getGeoInfo(ia: InetAddress): GeoInfo = {
      try {
        val c = geoip.city(ia)
        val builder = GeoInfo.newBuilder()
        Option(c.getCountry.getGeoNameId).foreach(x => builder.setCountry(x))
        c.getSubdivisions.asScala.map(x => Option(x.getGeoNameId)).flatten.headOption.foreach(x => builder.setRegion(x))
        Option(c.getCity.getGeoNameId).foreach(x => builder.setCity(x))

        builder.build()
      } catch {
        case e: Exception  =>
          Stats2.incr("error/geo/" + e.getClass.getSimpleName)
          //        log.error(s"Error during geo identification for ip '$ia'", e)
          GeoInfo.newBuilder().build()
      }
    }
}
