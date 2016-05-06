package ingestion

import java.time._
import java.net.InetSocketAddress
import java.time.format.DateTimeFormatter
import java.util.Date
import akka.actor.Props
import com.datastax.spark.connector._
import com.typesafe.config.Config
import domain.formats.DomainEventFormats.ResultAddedEvent
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import com.datastax.spark.connector.streaming._

import scala.collection.mutable

object JournalChangesIngestion {
  val CassandraPort = 9042
  val StreamingBatchInterval = 15
  val est = ZoneId.of("America/New_York")
  val Pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def start(ctx: SparkContext, config: Config,
            teams: scala.collection.mutable.HashMap[String, String]) = {
    import SparkFunctions._
    import scala.collection.JavaConverters._

    val streaming = new StreamingContext(ctx, Seconds(5))
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

    /*
                       +------------------+------------------------+
                       |26Apr2015:opponent|26Apr2015:opponent_score|
      +----------------+------------------+------------------------+
      |season-15-16:okc|     hou          |          93            |
      +----------------+------------------+------------------------+


     CREATE TABLE results_by_period (
        period text,
        team text,
        date timestamp,
        opponent text,
        opponent_score int,
        opponent_score_line text,
        score int,
        score_line text,
        seq_number bigint,
        PRIMARY KEY ((period, team), date)
    ) WITH CLUSTERING ORDER BY (date DESC);

    select * from results_by_period where period = 'season-12-13' and team = 'sas';

    */

    val journal = streaming.actorStream[(ResultAddedEvent, Long)](Props(new Journal(config, hosts, teams, gameIntervals)), "journal")

    journal.map(event ⇒ transformResult(event._1, event._2))
      .saveToCassandra(keySpace, resultsTable,
        SomeColumns("period", "team", "score_line", "score", "opponent", "opponent_score_line", "opponent_score", "date", "seq_number"))

    /*
                   +--------------------+--------------------+
                   |26Apr2015:J.Wall:pos|26Apr2015:J.Wall:min|
      +------------+--------------------+--------------------+
      |season-15-16|         guard      |      35:23         |
      +------------+--------------------+--------------------+

    CREATE TABLE leaders_by_period (
      period text,
      time timestamp,
      name text,
      ast int,
      ba int,
      blockshoot int,
      defreb int,
      fgma text,
      ftma text,
      min text,
      minusslashplus text,
      offreb int,
      opponent text,
      pf int,
      pos text,
      pts int,
      steel int,
      team text,
      threepma text,
      to0 int,
      totalreb int,
      PRIMARY KEY (period, time, name)
    ) WITH CLUSTERING ORDER BY (time ASC, name ASC);

    select name, team, pts from leaders_by_period where period = 'season-12-13' LIMIT 10;

    */

    journal.map(event ⇒ transformLeaders(event._1))
      .flatMapValues(lines ⇒ lines)
      .map { kv ⇒
        val line = kv._2
        (kv._1, line._1, line._2, line._3, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11, line._12, line._13, line._14, line._15, line._16, line._17, line._18, line._19, line._20)
      }
      .saveToCassandra(keySpace, leadersPlayers,
        SomeColumns("period", "name", "pos", "min", "fgma", "threepma", "ftma", "minusslashplus", "offreb", "defreb", "totalreb", "ast", "pf", "steel", "to0", "blockshoot", "ba", "pts",
          "team", "opponent", "time"))

    /*

                              +-------------+------------------+
                              |26Apr2015:pts|26Apr2015:opponent|
     +------------------------+-------------+------------------+
     |L.James:season-15-16:cle|27           |chi               |
     +------------------------+-------------+------------------+


     CREATE TABLE player_by_period_team (
        name text,
        period text,
        team text,
        time timestamp,
        ast int,
        ba int,
        blockshoot int,
        defreb int,
        fgma text,
        ftma text,
        min text,
        minusslashplus text,
        offreb int,
        opponent text,
        pf int,
        pos text,
        pts int,
        steel int,
        threepma text,
        to0 int,
        totalreb int,
        PRIMARY KEY ((name, period, team), time)
     ) WITH CLUSTERING ORDER BY (time ASC);

     select name, pts, team, opponent, time from player_by_period_team where name = 'S. Curry' and period = 'season-12-13' and team = 'gsw';

    */

    journal.map(event ⇒ transformPlayers(event._1)).flatMapValues(lines ⇒ lines)
      .map { kv ⇒
        val r = kv._2
        (r._1, kv._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11, r._12, r._13, r._14, r._15, r._16, r._17, r._18, r._19, r._20)
      }.saveToCassandra(keySpace, playersTable,
        SomeColumns("name", "period", "team", "time", "pos", "min", "fgma", "threepma", "ftma", "minusslashplus", "offreb", "defreb", "totalreb",
          "ast", "pf", "steel", "to0", "blockshoot", "ba", "pts", "opponent"))

    /*

                  +--------------------------+--------------------------------+--------------------------+
                  |2015:10:29:"okc-hou":score|2015:10:29:"okc-hou":guest_score|2015:10:29:"mia-cle":score|
     +---------------------------------------+--------------------------------+--------------------------+
     |season-15-16|89                        |89                              | 67                       |
     +---------------------------------------+-----------------------------------------------------------+


    CREATE TABLE daily_results (
        period text,
        opponents text,
        year int,
        month int,
        day int,
        score int,
        guest_score int,
        score_line text,
        guest_score_line text,
        PRIMARY KEY ((period), year, month, day, opponents)
    ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, opponents ASC);

    select * from daily_results where period = 'season-12-13' and year=2012  and month=11 and day=29;
    */

    journal.map(ev ⇒ transformResult2(ev._1))
      .saveToCassandra(keySpace, dailyResultsTable,
        SomeColumns("period", "opponents", "year", "month", "day", "score", "guest_score", "score_line", "guest_score_line"))

    streaming.start()
  }
}