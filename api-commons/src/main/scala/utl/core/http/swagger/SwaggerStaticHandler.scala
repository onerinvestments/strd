package utl.core.http.swagger

import org.slf4j.LoggerFactory
import utl.net.http.{StaticResourcesHandler, PartialPathHandler}

/**
 *
 * User: light
 * Date: 26/10/14
 * Time: 21:02
 */

class SwaggerStaticHandler extends PartialPathHandler with StaticResourcesHandler {

  override def tryHandle = {
    case "swg" :: Nil => classpathResource(List("swagger_ui", "index.html"), _)
    case "swg" :: a => classpathResource("swagger_ui" :: a, _)
  }

  override def log = LoggerFactory.getLogger(getClass)
}
