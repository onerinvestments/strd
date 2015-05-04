package strd.util

import java.text.{DateFormat, SimpleDateFormat}
import lmbrd.zn.util.TimeUtil
import java.util.Date
import org.slf4j.LoggerFactory


/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/26/13
 * Time: 4:02 PM
 */
object DateUtils {

  val log = LoggerFactory.getLogger(getClass)

  val formatWithYear = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue() = {
      new SimpleDateFormat("yyyy.M.d")
    }
  }


  val formatWithoutYear = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue() = {
      new SimpleDateFormat("M.d")
    }
  }

  val startDate = formatWithYear.get.parse("1890.1.1").getTime

  val localeFormatter = new ThreadLocal[DateFormat]() {
    override def initialValue() = DateFormat.getDateTimeInstance
  }


  def parseDate(date: String) = {

    if (date == null || date.isEmpty) {
      throw new NullPointerException("no date")
    }
    try {
      val dt = if (date.length > 5) {
        val format = formatWithYear.get
        if (format == null) throw new RuntimeException("Fuck :(")
        format.parse(date)
      } else {
        val format = formatWithoutYear.get
        if (format == null) throw new RuntimeException("Fuck :(")
        format.parse(date)
      }

      dateToIntHours(dt)
    } catch {
      case x : Exception => {
        log.error("Fucking exception :  " + date, x )
        throw x
      }
    }

  }

  def dateToIntHours(date: Date) = ((date.getTime - startDate) / TimeUtil.aHOUR).toInt

  def intHoursToDate(days: Int) = new Date(days * TimeUtil.aHOUR + startDate)

  def localeFormat(date: Date): String = localeFormatter.get.format(date)
  def localeFormat(ts: Long): String = localeFormat(new Date(ts))

  println(parseDate("11.11"))
  println(parseDate("1964.11.11"))
  println(parseDate("1940.11.11"))
  println(parseDate("2020.11.11"))

  println(intHoursToDate(parseDate("21.09")))
  println(intHoursToDate(parseDate("1904.11.11 ")))
  println(intHoursToDate(parseDate("1987.01.01 ")))
  println(intHoursToDate(parseDate("2020.11.11 ")))

}
