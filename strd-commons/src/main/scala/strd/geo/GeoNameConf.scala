package strd.geo

import strd.util.{StringParam, ConfigParamsDeclaration}

/**
 * Created by penkov on 13.02.15.
 */
class GeoNameConfParams extends ConfigParamsDeclaration {

  val JDBC_URL          = StringParam        ("geo.jdbc.url")
  val JDBC_USER         = StringParam        ("geo.jdbc.user")
  val JDBC_PWD          = StringParam        ("geo.jdbc.pwd")
  val GEO_NAMES_TABLE   = StringParam        ("geo.table", Some("geo_names"))



}


object GeoNameConf extends GeoNameConfParams {

}