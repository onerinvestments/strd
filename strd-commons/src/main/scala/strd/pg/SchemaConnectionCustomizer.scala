package strd.pg

import java.sql.{Connection, Statement}

import com.mchange.v2.c3p0.AbstractConnectionCustomizer
import org.slf4j.LoggerFactory

import scala.collection.mutable

object SchemaConnectionCustomizer {
  val schemas = new mutable.HashMap[String, String]()
}
class SchemaConnectionCustomizer extends AbstractConnectionCustomizer {
  val log = LoggerFactory.getLogger(getClass)

  def schema(parentDataSourceIdentityToken: String) = {
    SchemaConnectionCustomizer.schemas(parentDataSourceIdentityToken)
  }

  override def onAcquire(c: Connection, parentDataSourceIdentityToken: String) {
    val schemaName = schema(parentDataSourceIdentityToken)

    log.debug(s"Setting schema to $schemaName")

    var stmt: Statement = null
    try {
      stmt = c.createStatement()
      stmt.executeUpdate("SET search_path TO " + schemaName);
    } catch {
      case e: Exception =>
        log.warn(s"Can not set schema to $schemaName", e)
        throw e
    } finally {
      if (stmt != null) stmt.close()
    }
  }
}
