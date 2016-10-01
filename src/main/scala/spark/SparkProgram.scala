package spark

import java.util.Date
import akka.actor.{ Actor, ActorLogging, Props }
import com.typesafe.config.Config
import http._
import org.apache.spark.SparkContext
import spark.DailyResultsQuery._
import scala.collection.mutable

object SparkProgram {

  sealed trait JobManagerProtocol

  case class StreamJobSubmit(job: String) extends JobManagerProtocol

  case class StandingQueryArgs(ctx: SparkContext, url: String, stage: String,
    teams: mutable.HashMap[String, String], period: String)
      extends JobManagerProtocol with DefaultJobArgs

  case class TeamResultsQueryArgs(ctx: SparkContext, url: String, period: String, teams: scala.collection.Seq[String],
    arenas: Seq[(String, String)], allTeams: mutable.HashMap[String, String])
      extends JobManagerProtocol with DefaultJobArgs

  case class PtsLeadersQueryArgs(ctx: SparkContext, url: String, teams: mutable.HashMap[String, String],
    interval: String, depth: Int) extends JobManagerProtocol with DefaultJobArgs

  case class RebLeadersQueryArgs(ctx: SparkContext, url: String, period: String, depth: Int)
    extends JobManagerProtocol with DefaultJobArgs

  case class PlayerStatsQueryArgs(ctx: SparkContext, url: String, name: String, period: String, team: String)
    extends JobManagerProtocol with DefaultJobArgs

  case class DailyResultsQueryArgs(ctx: SparkContext, url: String, stage: String, yyyyMMdd: (Int, Int, Int),
    arenas: Vector[(String, String)], teams: mutable.HashMap[String, String])
      extends JobManagerProtocol with DefaultJobArgs

  case class Standing(team: String = "", hw: Int = 0, hl: Int = 0, aw: Int = 0, al: Int = 0, w: Int = 0, l: Int = 0)

  case class ResultView(lineup: String, score: String, time: String, arena: String)

  case class PtsLeader(team: String, player: String, pts: Double, games: Long = 0)

  case class RebLeader(team: String = "", player: String = "", offensive: Double, defensive: Double, total: Double, games: Long = 0)

  case class Stats(VS: String, DT: Date, MIN: String, REB_DEF: Int, REB_OFF: Int, REB_TOTAL: Int, FGM_A: String, PM3_A: String,
    FTM_A: String, ASSISTS: Int, BL: Int, STEEL: Int, PLUS_MINUS: String, PTS: Int)

  sealed trait SparkQueryView extends DefaultResponseBody {
    def count: Int

    def latency: Long

    def error: Option[String]
  }

  case class SeasonStandingView(count: Int = 0, west: List[Standing] = List.empty,
    east: List[Standing] = List.empty, latency: Long = 0l, error: Option[String] = None)
      extends SparkQueryView

  case class PlayoffStandingView(count: Int = 0, table: List[String], latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class FilteredView(count: Int = 0, results: List[ResultView] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class FilteredAllView(count: Int = 0, results: List[ResultView] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class PtsLeadersView(count: Int = 0, leaders: List[PtsLeader] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class RebLeadersView(count: Int = 0, leaders: List[RebLeader] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class PlayerStatsView(count: Int = 0, stats: List[Stats] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class ResultsView(count: Int = 0, stats: List[ResultView] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  case class DailyResultsView(count: Int = 0, results: List[ResultView] = List.empty, latency: Long = 0l,
    error: Option[String] = None) extends SparkQueryView

  val Season = "season"
  val PlayOff = "playoff"

  def props(config: Config): Props = Props(new SparkProgram(config)).withDispatcher(SparkDispatcher)
}

class SparkProgram(val config: Config) extends Actor with ActorLogging {
  import SparkProgram._

  val sys = context.system
  implicit val ex = context.system.dispatchers.lookup(SparkDispatcher)

  override def preStart = {
    //context.setReceiveTimeout(30 milliseconds)
    log.info("SparkProgram has been created")
  }

  override def postStop =
    log.info("SparkProgram has been stopped")

  import akka.pattern.pipe
  override def receive: Receive = {
    case TeamResultsQueryArgs(ctx, _, period, teams, arenas, allTeams) ⇒
      log.info(
        s"SELECT team, score, opponent, opponent_score, date FROM results_by_period WHERE period = '{}' and team in ({})",
        period, teams.map(t => s"""'$t',""").mkString
      )
      val replyTo = sender()
      (TeamsResultsQuery[ResultsView] async (ctx, config, period, teams, arenas, allTeams) to replyTo).future
        .onComplete(_ ⇒ context.stop(self))

    case DailyResultsQueryArgs(ctx, url, stage, yyyyMMDD, arenas, teams) ⇒
      log.info("SELECT * FROM daily_results WHERE period = '{}' and year={} and month={} and day={}", stage, yyyyMMDD._1, yyyyMMDD._2, yyyyMMDD._3)
      val replyTo = sender()
      (DailyResultsQuery[DailyResultsView].async(ctx, config, stage, yyyyMMDD, arenas, teams) to replyTo).future
        .onComplete(_ ⇒ context.stop(self))

    case PlayerStatsQueryArgs(ctx, url, name, period, team) ⇒
      log.info(
        "SELECT name, pts, team, opponent, time FROM player_by_name WHERE name = '{}' and period = '{}' and team = '{}'",
        name, period, team
      )
      val replyTo = sender()
      (spark.PlayerStatsQuery[PlayerStatsView] async (ctx, config, name, period, team) to replyTo).future
        .onComplete(_ ⇒ context.stop(self))

    case PtsLeadersQueryArgs(ctx, url, teams, period, depth) ⇒
      log.info("SELECT name, team, pts FROM leaders_by_period WHERE period = '{}'", period)
      val replyTo = sender()
      (PtsLeadersQuery[PtsLeadersView].async(ctx, config, period, depth) to replyTo).future
        .onComplete(_ ⇒ context.stop(self))

    case RebLeadersQueryArgs(ctx, url, period, depth) ⇒
      log.info("SELECT name, team, offreb, defreb, totalreb FROM leaders_by_period where period = '{}'", period)
      val replyTo = sender()
      (RebLeadersQuery[RebLeadersView] async (ctx, config, period, depth) to replyTo).future
        .onComplete(_ ⇒ context.stop(self))

    case StandingQueryArgs(ctx, _, stage, teams, period) ⇒
      log.info(
        "SELECT team, score, opponent, opponent_score, date FROM results_by_period WHERE period = '{}' and team in ({})",
        stage, teams.keySet.map(t => s"""'$t',""").mkString
      )
      val replyTo = sender()
      if (stage contains Season)
        ((StandingQuery[SeasonStandingView] async (ctx, config, teams, period)) to replyTo).future
          .onComplete(_ ⇒ context.stop(self))
      else if (stage contains PlayOff)
        ((StandingQuery[PlayoffStandingView] async (ctx, config, teams, period)) to replyTo).future
          .onComplete(_ ⇒ context.stop(self))

  }
}
