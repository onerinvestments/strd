package strd.util

import org.slf4j.LoggerFactory

/**
 * @author Kirill chEbba Chebunin
 */
trait Loggable {
  protected implicit lazy val log = LoggerFactory.getLogger(getClass)
}
