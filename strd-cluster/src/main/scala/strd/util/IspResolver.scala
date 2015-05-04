package strd.util

import java.io.File
import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader

/**
 * @author Kirill chEbba Chebunin
 */
class IspResolver(file: String) {
  val reader = new DatabaseReader.Builder(new File(file)).build()

  def resolve(ip: InetAddress): Option[Isp] = {
    val isp = reader.isp(ip)
    Option(isp.getIsp).map(Isp(_, isp.getOrganization))
  }

  def resolve(ip: String): Option[Isp] = resolve(InetAddress.getByName(ip))
}

object IspResolver {
  val DEFAULT_FILE = "/usr/share/mmdb/GeoIP2-ISP.mmdb"

  def apply(file: String = DEFAULT_FILE) = new IspResolver(file)
}

case class Isp(name: String, organization: String)
