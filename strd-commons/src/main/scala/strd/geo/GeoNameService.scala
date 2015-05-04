package strd.geo

import java.sql.{ResultSet, PreparedStatement}

import org.apache.commons.lang.StringUtils
import org.springframework.jdbc.core.{RowMapper, PreparedStatementSetter, JdbcTemplate}
import strd.util.ClusterConfigs
import scala.collection.convert.decorateAsScala._

/**
 *
 * Created by penkov on 13.02.15.
 */

trait GeoNameService {
  def resolveNames(countriesIds: Seq[Int]): Seq[GeoName]
}

class GeoNameServiceImpl(implicit val conf: ClusterConfigs) extends GeoNameService {


  val jdbcTemplate = new JdbcTemplate(conf.pooledDS(3, GeoNameConf.JDBC_USER.id, GeoNameConf.JDBC_PWD.id, GeoNameConf.JDBC_URL.id))

  def resolveNames(countriesIds: Seq[Int]): Seq[GeoName] = {

    if (countriesIds.isEmpty) {
      Seq.empty
    }
    else {

      countriesIds.sliding(100, 100).map{ ids =>
        jdbcTemplate.query("select country_id, parent_id, name, ru_name from " + GeoNameConf(_.GEO_NAMES_TABLE) + " where country_id in ("+StringUtils.repeat("?", ",", ids.size)+")", new PreparedStatementSetter {
          def setValues(ps: PreparedStatement) {
            ids.zipWithIndex.foreach { case (value, idx) => ps.setInt(idx + 1, value) }
          }
        }, new RowMapper[GeoName] {
          def mapRow(rs: ResultSet, rowNum: Int) = {
            val countryId = rs.getInt(1)

            val parentIdParam = rs.getInt(2)
            val parentId = if (rs.wasNull()) None else Some(parentIdParam)

            val name = rs.getString(3)

            val ruNameParam = rs.getString(4)
            val ruName = if (rs.wasNull()) None else Some(ruNameParam)
            GeoName(countryId, parentId, name, ruName)
          }

        }

        ).asScala.toSeq
      }


    }.flatten.toIndexedSeq


  }
}


