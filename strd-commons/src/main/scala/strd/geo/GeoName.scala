package strd.geo

/**
 * Created by penkov on 13.02.15.
 */
case class GeoName(countryId: Int, parentId: Option[Int], name: String, ruName: Option[String])
