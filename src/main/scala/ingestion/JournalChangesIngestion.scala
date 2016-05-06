package ingestion

import java.time._
import java.net.InetSocketAddress
import java.time.format.DateTimeFormatter
import java.util.Date
import akka.actor.Props
import com.typesafe.config.Config
import domain.formats.DomainEventFormats.ResultAddedEvent
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import com.datastax.spark.connector.streaming._

import scala.collection.mutable

object JournalChangesIngestion extends CassandraSchema {
  val CassandraPort = 9042
  val StreamingBatchInterval = 15
  val est = ZoneId.of("America/New_York")
  val Pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def start(ctx: SparkContext, config: Config,
            teams: scala.collection.mutable.HashMap[String, String]) = {
    import SparkFunctions._
    import scala.collection.JavaConverters._

    val streaming = new StreamingContext(ctx, Seconds(StreamingBatchInterval))
    streaming.checkpoint("scenter-checkpoint")

    val hosts = config.getString("db.cassandra.seeds").split(",").map(host ⇒ new InetSocketAddress(host, CassandraPort))
    val resultsTable = config.getString("spark.cassandra.journal.results")
    val leadersPlayers = config.getString("spark.cassandra.journal.leaders")
    val playersTable = config.getString("spark.cassandra.journal.player")
    val dailyResultsTable = config.getString("spark.cassandra.journal.daily")
    val keySpace = config.getString("spark.cassandra.journal.keyspace")

    val gameIntervals = intervals(config.getConfig("app-settings").getObjectList("stages").asScala
      ./:(scala.collection.mutable.LinkedHashMap[String, String]()) { (acc, c) ⇒
        val it = c.entrySet().iterator()
        if (it.hasNext) {
          val entry = it.next()
          acc += (entry.getKey -> entry.getValue.render().replace("\"", ""))
        }
        acc
      }, est, Pattern)

    val transformResult2: (ResultAddedEvent) ⇒ ((String, String, Int, Int, Int, Int, Int, String, String)) =
      (event) ⇒ {
        val periodFinder = findPeriod
        val intervals = gameIntervals
        val eventTime = ZonedDateTime.of(LocalDateTime.ofInstant(new Date(event.getResult.getTime).toInstant(), est), est)
        (periodFinder(intervals.entrySet().iterator(), eventTime),
          s"${event.getResult.getHomeTeam} - ${event.getResult.getAwayTeam}",
          eventTime.getYear, eventTime.getMonthValue, eventTime.getDayOfMonth,
          event.getResult.getHomeScore, event.getResult.getAwayScore, event.getResult.getHomeScoreLine, event.getResult.getAwayScoreLine)
      }

    val transformResult: (ResultAddedEvent, Long) ⇒ ((String, String, String, Int, String, String, Int, Date, Long)) =
      (event, count) ⇒ {
        val local = findPeriod
        val intervalsLocal = gameIntervals
        val eventTime = ZonedDateTime.of(LocalDateTime.ofInstant(new Date(event.getResult.getTime).toInstant(), est), est)
        (local(intervalsLocal.entrySet().iterator(), eventTime),
          event.getResult.getHomeTeam, event.getResult.getHomeScoreLine, event.getResult.getHomeScore,
          event.getResult.getAwayTeam, event.getResult.getAwayScoreLine, event.getResult.getAwayScore,
          new Date(event.getResult.getTime), count)
      }

    val transformLeaders: (ResultAddedEvent) ⇒ ((String, mutable.Buffer[((String, String, String, String, String, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, String, String, Date))])) =
      (event) ⇒ {
        val local = findPeriod
        val intervalsLocal = gameIntervals
        val eventTime = ZonedDateTime.of(LocalDateTime.ofInstant(new Date(event.getResult.getTime).toInstant(), est), est)
        val period = local(intervalsLocal.entrySet().iterator(), eventTime)

        val scoreBoxes = event.getResult.getHomeScoreBoxList.asScala.map(l ⇒ (l.getName, l.getPos, l.getMin, l.getFgmA, l.getThreePmA, l.getFtmA, l.getMinusSlashPlus, l.getOffReb, l.getDefReb,
          l.getTotalReb, l.getAst, l.getPf, l.getSteels, l.getTo, l.getBs, l.getBa, l.getPts, event.getResult.getHomeTeam, event.getResult.getAwayTeam, new Date(event.getResult.getTime))) ++
          event.getResult.getAwayScoreBoxList.asScala.map(l ⇒ (l.getName, l.getPos, l.getMin, l.getFgmA, l.getThreePmA, l.getFtmA, l.getMinusSlashPlus, l.getOffReb, l.getDefReb,
            l.getTotalReb, l.getAst, l.getPf, l.getSteels, l.getTo, l.getBs, l.getBa, l.getPts, event.getResult.getAwayTeam, event.getResult.getHomeTeam, new Date(event.getResult.getTime)))

        (period, scoreBoxes)
      }

    val transformPlayers: (ResultAddedEvent) ⇒ ((String, mutable.Buffer[(String, String, Date, String, String, String, String, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, String)])) =
      event ⇒ {
        val local = findPeriod
        val intervalsLocal = gameIntervals
        val eventTime = ZonedDateTime.of(LocalDateTime.ofInstant(new Date(event.getResult.getTime).toInstant(), est), est)
        val period = local(intervalsLocal.entrySet().iterator(), eventTime)
        val r = event.getResult

        val playersBox = r.getHomeScoreBoxList.asScala.map(b ⇒ (b.getName, r.getHomeTeam, new Date(r.getTime), b.getPos, b.getMin, b.getFgmA, b.getThreePmA, b.getFtmA,
          b.getMinusSlashPlus, b.getOffReb, b.getDefReb, b.getTotalReb, b.getAst, b.getPf, b.getSteels, b.getTo, b.getBs, b.getBa, b.getPts, r.getAwayTeam)) ++
          r.getAwayScoreBoxList.asScala.map(b ⇒ (b.getName, r.getAwayTeam, new Date(r.getTime), b.getPos, b.getMin, b.getFgmA, b.getThreePmA, b.getFtmA,
            b.getMinusSlashPlus, b.getOffReb, b.getDefReb, b.getTotalReb, b.getAst, b.getPf, b.getSteels, b.getTo, b.getBs, b.getBa, b.getPts, r.getHomeTeam))
        (period, playersBox)
      }

    installSchema(ctx.getConf, keySpace, resultsTable, leadersPlayers, playersTable, dailyResultsTable, teams)

    val journal = streaming.actorStream[(ResultAddedEvent, Long)](Props(new Journal(config, hosts, teams, gameIntervals)), "journal")

    journal.map(event ⇒ transformResult(event._1, event._2)).saveToCassandra(keySpace, resultsTable, resultColumns)

    journal.map(event ⇒ transformLeaders(event._1))
      .flatMapValues(lines ⇒ lines)
      .map { kv ⇒
        val line = kv._2
        (kv._1, line._1, line._2, line._3, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11, line._12, line._13, line._14, line._15, line._16, line._17, line._18, line._19, line._20)
      }
      .saveToCassandra(keySpace, leadersPlayers, leadersColumns)


    journal.map(event ⇒ transformPlayers(event._1)).flatMapValues(lines ⇒ lines)
      .map { kv ⇒
        val r = kv._2
        (r._1, kv._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11, r._12, r._13, r._14, r._15, r._16, r._17, r._18, r._19, r._20)
      }
      .saveToCassandra(keySpace, playersTable, playerColumns)

    journal.map(ev ⇒ transformResult2(ev._1)).saveToCassandra(keySpace, dailyResultsTable, dailyResColumns)

    streaming.start()
  }
}