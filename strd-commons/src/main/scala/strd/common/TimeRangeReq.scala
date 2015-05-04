package strd.common

import lmbrd.zn.util.TimeUtil.TimeGroupType
import strd.util.DateUtils

/**
 *
 * User: light
 * Date: 21/10/14
 * Time: 19:38
 */
case class TimeRangeReq(from: Long, to: Long, tg: TimeGroupType) {
  override def toString = s"${getClass.getSimpleName}(${DateUtils.localeFormat(from)} -> ${DateUtils.localeFormat(to)} $tg)"
}
