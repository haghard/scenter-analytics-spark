package http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import spark.{SparkQuerySupervisor, SparkJob}
import SparkJob._
import http.ResultsRouter.TeamsHttpProtocols

import javax.ws.rs.Path

import io.swagger.annotations._
import org.apache.spark.SparkContext
import spray.json._

import scala.concurrent.{ ExecutionContext, Future }

object ResultsRouter {
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
          case Some(ResultsView(c, results, latency, _)) ⇒
            JsObject("url" -> url, "view" -> JsArray(results.map(_.toJson).toVector),
              "body" -> JsObject("count" -> JsNumber(c)),
              "latency" -> JsNumber(latency), "error" -> error)
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }
  }
}

@io.swagger.annotations.Api(value = "/results", produces = "application/json")
@Path("/api/results")
class ResultsRouter(override val host: String, override val httpPort: Int,
  override val intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
  override val teams: scala.collection.mutable.HashMap[String, String],
  override val httpPrefixAddress: String = "results",
  arenas: scala.collection.immutable.Vector[(String, String)],
  context: SparkContext)(implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport
    with TypedAsk with ParamsValidation with TeamsHttpProtocols {

  import cats.data.Xor
  import cats.data.Validated
  import cats.implicits._
  import scala.concurrent.duration._

  override implicit val timeout = akka.util.Timeout(10.seconds)

  private val jobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  val route = teamsRoute

  import akka.http.scaladsl.server._

  @Path("/{stage}")
  @ApiOperation(value = "Fetch results by season and teams", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "stage", value = "Name of a stage. Examples:season-14-15 or playoff-14-15", required = true, dataType = "string", paramType = "path"),
    new ApiImplicitParam(name = "teams", value = "Teams separated by commas. Examples:cle,okc,hou,chi", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "Authorization", value = "Authorization token", required = true, dataType = "string", paramType = "header")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[ResultsView]),
    new ApiResponse(code = 403, message = "The supplied authentication is not authorized to access this resource"),
    new ApiResponse(code = 404, message = "Unsupported season or team"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def teamsRoute(): Route =
    pathPrefix(pathPrefix) {
      (get & path(httpPrefixAddress / Segment)) { stage ⇒
        parameters(('teams.as[String])) { teams ⇒
          requiredHttpSession(ec) { session ⇒
            withUri { url ⇒
              system.log.info(s"[user:${session.user}] accesses resource: [$url]")
              get(complete(search(url, stage, teams)))
            }
          }
        }
      }
    }

  private def search(url: String, season: String, searchTeams: String): Future[HttpResponse] = {
    val validation = cats.Apply[Validated[String, ?]].map2(
      validateTeams(searchTeams),
      validatePeriod(season)
    ) { case (_, _) => TeamResultsQueryArgs(context, url, season, searchTeams.split(",").toSeq, arenas, teams) }

    validation.fold({ error => Future.successful(notFound(s"Invalid parameters: $error")) }, { arg =>
      fetch[ResultsView](arg, jobSupervisor).map {
        case Xor.Right(res) => success(SparkJobHttpResponse(url, view = Option("team-results"), body = Option(res), error = res.error))
        case Xor.Left(er) => internalError(er)
      }
    })
  }
}