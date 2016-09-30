package http

import java.net.URLDecoder

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import http.PlayersRouter.PlayersProtocol
import http.SparkJob.{Stats, PlayerStatsView, PlayerStatsQueryArgs}
import org.apache.spark.SparkContext
import spray.json._
import scala.concurrent.{Future, ExecutionContext}
import io.swagger.annotations._
import javax.ws.rs.Path

object PlayersRouter {

  trait PlayersProtocol extends StandingHttpProtocols {

    implicit val statsFormat = jsonFormat14(Stats.apply)

    implicit object PlayersResponseWriter
        extends JsonWriter[SparkJobHttpResponse] {
      import spray.json._
      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒
          JsString(view)
        }
        val error = obj.error.fold(JsString("none")) { error ⇒
          JsString(error)
        }
        obj.body match {
          case Some(PlayerStatsView(c, stats, latency, _)) ⇒
            JsObject("url" -> url,
                     "view" -> JsArray(stats.map(_.toJson)),
                     "latency" -> JsNumber(latency),
                     "body" -> JsObject("count" -> JsNumber(c)),
                     "error" -> error)
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }
  }
}

@io.swagger.annotations.Api(value = "/player/stats", produces = "application/json")
@Path("/api/player/stats")
class PlayersRouter(override val host: String, override val httpPort: Int,
                   context: SparkContext,
                   intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
                   arenas: scala.collection.immutable.Vector[(String, String)],
                   teams: scala.collection.mutable.HashMap[String, String],
                   override val httpPrefixAddress: String = "player")
                  (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport with ParamsValidation with TypedAsk with PlayersProtocol {
  private val enc = "utf-8"
  private val playerJobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  //http GET [host]:[port]/api/player/stats?"name=S. Curry&period=season-15-16&team=gsw" Authorization:...
  @ApiOperation(value = "Fetch results by player", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "name", value = "Player name", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "period", value = "Period", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "team", value = "Team", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "Authorization", value = "Authorization token", required = true, dataType = "string", paramType = "header")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[PlayerStatsView]),
    new ApiResponse(code = 403, message = "The supplied authentication is not authorized to access this resource"),
    new ApiResponse(code = 404, message = "Unsupported season or team"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def dailyRoute(): Route =
    pathPrefix(pathPrefix) {
      path(httpPrefixAddress / "stats") {
        get {
          parameters(('name.as[String]), ('period.as[String]), ('team.as[String])) { (name, period, team) ⇒
            withUri { url ⇒
              requiredHttpSession(ec) { session ⇒
                system.log.info(s"[user:${session.user}] access [$host:$httpPort/$pathPrefix/$httpPrefixAddress/stats]")
                get(complete(playerStats(URLDecoder.decode(url, enc), URLDecoder.decode(name, enc), period, team)))
              }
            }
          }
        }
      }
    }

  private def playerStats(url: String, name: String, period: String, team: String)
                         (implicit ex: ExecutionContext): Future[HttpResponse] = {
    val validation = cats.Apply[cats.data.Validated[String, ?]].map2(
      validateTeam(team), validatePeriod(period)
    ) { case (_, _) => PlayerStatsQueryArgs(context, url, name, period, team) }

    validation.fold({ error => Future.successful(notFound(s"Invalid parameters: $error")) }, { arg =>
      fetch[PlayerStatsView](arg, playerJobSupervisor).map {
        case cats.data.Xor.Right(res) ⇒ success(SparkJobHttpResponse(url, view = Option("player-stats"), body = Option(res), error = res.error))(PlayersResponseWriter)
        case cats.data.Xor.Left(ex) ⇒ internalError(ex)
      }
    })
  }
}
