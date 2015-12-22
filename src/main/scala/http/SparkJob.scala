package http

import java.util.Date

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import akka.actor.{ Actor, ActorLogging, Props }
import spark.PtsLeadersQuery._
import spark._

import scala.collection.mutable
import akka.pattern.pipe

object SparkJob {

  sealed trait JobManagerProtocol
  case class StreamJobSubmit(job: String) extends JobManagerProtocol

  case class StandingSearchArgs(ctx: SparkContext, url: String, stage: String, teams: mutable.HashMap[String, String], period: String)
    extends JobManagerProtocol with DefaultJobArgs

  case class TeamStatQueryArgs(ctx: SparkContext, url: String, period: String,
                               teams: scala.collection.Seq[String],
                               arenas: Seq[(String, String)],
                               allTeams: mutable.HashMap[String, String])
      extends JobManagerProtocol with DefaultJobArgs

  case class PtsLeadersSearchArgs(ctx: SparkContext, url: String, stage: String, teams: mutable.HashMap[String, String], interval: String, depth: Int)
    extends JobManagerProtocol with DefaultJobArgs

  case class RebLeadersSearchArgs(ctx: SparkContext, url: String, period: String, depth: Int)
    extends JobManagerProtocol with DefaultJobArgs

  case class PlayerStatsJobArgs(ctx: SparkContext, url: String, name: String, period: String, team: String) extends JobManagerProtocol with DefaultJobArgs

  case class Standing(team: String = "", hw: Int = 0, hl: Int = 0, aw: Int = 0, al: Int = 0, w: Int = 0, l: Int = 0) extends Serializable
  case class ResultView(lineup: String, score: String, time: String, arena: String) extends Serializable
  case class PtsLeader(team: String, player: String, pts: Double, games: Long = 0) extends Serializable
  case class RebLeader(team: String = "", player: String = "", offensive: Double, defensive: Double, total: Double, games: Long = 0) extends Serializable

  case class Stats(VS: String, DT: Date, MIN: String, REB_DEF: Int, REB_OFF: Int, REB_TOTAL: Int, FGM_A: String, PM3_A: String, FTM_A: String,
                   ASSISTS: Int, BL: Int, STEEL: Int, PLUS_MINUS: String, PTS: Int) extends Serializable

  trait SparkQueryView extends DefaultResponseBody {
    def count: Int
    def latency: Long
    def error: Option[String]
  }

  case class SeasonStandingView(count: Int = 0, west: List[Standing] = List.empty, east: List[Standing] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView
  case class PlayoffStandingView(count: Int = 0, table: List[String], latency: Long = 0l, error: Option[String] = None) extends SparkQueryView
  case class FilteredView(count: Int = 0, results: List[ResultView] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView
  case class FilteredAllView(count: Int = 0, results: List[ResultView] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView
  case class PtsLeadersView(count: Int = 0, leaders: List[PtsLeader] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView
  case class RebLeadersView(count: Int = 0, leaders: List[RebLeader] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView
  case class PlayerStatsView(count: Int = 0, stats: List[Stats] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView

  case class ResultView0(home: String, hs: Int, away: String, as: Int, dt: Date)
  case class TeamStatsView(count: Int = 0, stats: List[ResultView] = List.empty, latency: Long = 0l, error: Option[String] = None) extends SparkQueryView

  val Season = "season"
  val PlayOff = "playoff"

  def props(config: Config): Props = Props(new SparkJob(config)).withDispatcher(SparkDispatcher)
}

class SparkJob(val config: Config) extends Actor with ActorLogging {
  import SparkJob._
  implicit val Ex = context.dispatcher

  override def receive: Receive = {
    case PtsLeadersSearchArgs(ctx, url, stage, teams, interval, depth) ⇒
      log.info(s"Start spark pts-leader query with [${interval.toString}] with $depth")
      (PtsLeadersQuery[PtsLeadersView] async (ctx, config, interval, depth)) to sender()

    case StandingSearchArgs(ctx, _, stage, teams, period) ⇒
      log.info(s"Start spark standing query for [$stage]")
      if (stage contains Season) {
        (StandingQuery[SeasonStandingView] async (ctx, config, teams, period)) to sender()
      } else if (stage contains PlayOff) {
        (StandingQuery[PlayoffStandingView] async (ctx, config, teams, period)) to sender()
      }

    case PlayerStatsJobArgs(ctx, url, name, period, team) ⇒
      log.info(s"Start spark player-stat query for [$name]:[$team]:[$period]")
      PlayerStatsQuery[PlayerStatsView] async (ctx, config, name, period, team) to sender()

    case RebLeadersSearchArgs(ctx, url, period, depth) ⇒
      log.info(s"Start spark reb-leader query for [$period]")
      RebLeadersQuery[RebLeadersView] async (ctx, config, period, depth) to sender()

    case TeamStatQueryArgs(ctx, _, period, teams, arenas, allTeams) ⇒
      log.info(s"Start spark team-stats query for [$period] [$teams]")
      TeamStatsQuery[TeamStatsView] async (ctx, config, period, teams, arenas, allTeams) to sender()
      context.stop(self)
  }
}