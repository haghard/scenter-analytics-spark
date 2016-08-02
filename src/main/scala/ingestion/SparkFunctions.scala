package ingestion

import java.time
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}

object SparkFunctions extends Serializable {

  case class Period(start: ZonedDateTime, end: ZonedDateTime)
      extends Serializable

  val findPeriod: (java.util.Iterator[java.util.Map.Entry[Period, String]],
                   ZonedDateTime) ⇒ String =
    (iter: java.util.Iterator[java.util.Map.Entry[Period, String]],
     eventTime: ZonedDateTime) ⇒ {
      if (iter.hasNext) {
        val entry = iter.next()
        val period = entry.getKey
        if (period.start.isBefore(eventTime) && period.end.isAfter(eventTime))
          entry.getValue
        else findPeriod(iter, eventTime)
      } else "unknown"
    }

  val intervals: (scala.collection.mutable.LinkedHashMap[String, String],
                  ZoneId, DateTimeFormatter) ⇒ java.util.LinkedHashMap[
      Period,
      String] = (stages, est, pattern) ⇒ {
    val periods = new java.util.LinkedHashMap[Period, String]
    var start: Option[ZonedDateTime] = None
    var end: Option[ZonedDateTime] = None
    var period: Option[String] = None

    for ((k, v) ← stages) {
      if (start.isEmpty) {
        start = Option(
            ZonedDateTime.of(LocalDateTime.of(LocalDate.parse(v, pattern),
                                              time.LocalTime.of(23, 59, 59)),
                             est))
        period = Option(k)
      } else {
        end = Option(
            ZonedDateTime.of(LocalDateTime.of(LocalDate.parse(v, pattern),
                                              time.LocalTime.of(23, 59, 59)),
                             est))
        val interval = Period(start.get, end.get)
        periods.put(interval, period.get)
        start = Option(end.get)
        period = Option(k)
      }
    }
    periods
  }
}
