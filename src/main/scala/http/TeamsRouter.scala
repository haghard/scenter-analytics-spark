package http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import http.SparkJob._
import http.TeamsRouter.TeamsHttpProtocols

import javax.ws.rs.Path

import io.swagger.annotations._
import org.apache.spark.SparkContext
import spray.json._

import scala.concurrent.{ ExecutionContext, Future }

object TeamsRouter {

  sealed abstract class ParamsError
  final case class WrongTeam(field: String) extends ParamsError
  final case class ParseError(field: String) extends ParamsError

  trait TeamsHttpProtocols extends DefaultJsonProtocol {
    implicit val resFormat = jsonFormat4(ResultView)

    implicit object DateFormatToJson extends JsonFormat[java.util.Date] with DefaultJsonProtocol {
      import spray.json._

      val formatter = cassandra.extFormatter

      override def read(json: JsValue): java.util.Date = formatter.parse(json.convertTo[String])

      override def write(date: java.util.Date) = formatter.format(date).toJson
    }

    implicit object TeamsResponseWriter extends JsonWriter[SparkJobHttpResponse] {
      import spray.json._

      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { JsString(_) }
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error) }
        obj.body match {
          case Some(TeamStatsView(c, results, latency, _)) ⇒
            JsObject("url" -> url, "view" -> JsArray(results.map(_.toJson).toVector),
              "body" -> JsObject("count" -> JsNumber(c)),
              "latency" -> JsNumber(latency), "error" -> error)
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }
  }
}

@io.swagger.annotations.Api(value = "/teams", produces = "application/json")
@Path("/api/teams")
class TeamsRouter(override val host: String, override val httpPort: Int,
    context: SparkContext,
    intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
    arenas: scala.collection.immutable.Vector[(String, String)],
    teams: scala.collection.mutable.HashMap[String, String],
    override val httpPrefixAddress: String = "teams")(implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport with TypedAsk with TeamsHttpProtocols {
  import scala.concurrent.duration._

  override implicit val timeout = akka.util.Timeout(10.seconds)

  import cats.data.Xor
  import cats.data.Validated
  import cats.implicits._

  val T = implicitly[cats.Traverse[List]]

  private val jobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  /*abstract override def configureApi() = {
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ teamsRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ $teamsServicePath-routes was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/$pathPrefix/$teamsServicePath/{stage}?teams=...,... Authorization:...]")
  }*/

  val route = teamsRoute

  //season-15-16?"teams=cle,okc" Authorization:...
  import akka.http.scaladsl.server._

  @Path("/{season}")
  @ApiOperation(value = "Fetch results by season and teams", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "season", value = "Name of season. Examples:season-14-15 or playoff-14-15", required = true, dataType = "string", paramType = "path"),
    new ApiImplicitParam(name = "teams", value = "Teams separated by commas. Examples:cle,okc or hou,chi", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "Authorization", value = "Authorization token", required = true, dataType = "string", paramType = "header")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[TeamStatsView]),
    new ApiResponse(code = 403, message = "The supplied authentication is not authorized to access this resource"),
    new ApiResponse(code = 404, message = "Unsupported season or team"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def teamsRoute(): Route =
    pathPrefix(pathPrefix) {
      (get & path(httpPrefixAddress / Segment)) { period ⇒
        parameters(('teams.as[String])) { teams ⇒
          requiredHttpSession(ec) { session ⇒
            withUri { url ⇒
              system.log.info(s"[user:${session.user}] accesses resource: [$httpPrefixAddress/$pathPrefix/$httpPrefixAddress/{season}]")
              get(complete(search(url, period, teams)))
            }
          }
        }
      }
    }

  private def validateTeams(searchTeams: String): Validated[String, List[String]] = {
    val teamsFilter = searchTeams.split(",").toList
    T.traverseU(teamsFilter) { team =>
      if(teams.keySet.contains(team)) Validated.invalid[String, String](s"\n Could'n find team $team")
      else Validated.valid[String, String](team)
    }
  }

  private def validatePeriod(season: String): Validated[String, Unit] = {
    (for { (k, v) ← intervals if (v == season) } yield k).headOption
      .fold(Validated.invalid[String, Unit](s"\n Could'n find season $season"))(_ => Validated.valid[String, Unit](()))
  }

  private def search(url: String, season: String, searchTeams: String): Future[HttpResponse] = {
    val validation = cats.Apply[Validated[String, ?]].map2(
      validateTeams(searchTeams),
      validatePeriod(season)
    ) { case (tms, seas) => TeamStatQueryArgs(context, url, season, tms.toSeq, arenas, teams) }

    validation.fold({ error => Future.successful(notFound(s"Invalid parameters: $error")) }, { arg =>
      fetch[TeamStatsView](arg, jobSupervisor).map {
        case Xor.Right(res) => success(SparkJobHttpResponse(url, view = Option("teams-stats"), body = Option(res), error = res.error))
        case Xor.Left(er) => internalError(er)
      }
    })
  }
}