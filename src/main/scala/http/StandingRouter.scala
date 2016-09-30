package http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import http.ResultsRouter.TeamsHttpProtocols
import http.SparkJob._
import org.apache.spark.SparkContext
import spray.json.JsonWriter
import scala.concurrent.{Future, ExecutionContext}
import spray.json._

object StandingRouter {

  case class SparkJobHttpResponse(url: String,view: Option[String] = None,
                                  body: Option[SparkQueryView] = None,  error: Option[String] = None) extends DefaultHttpResponse

  trait StandingHttpProtocols extends TeamsHttpProtocols {
    implicit val standingFormat = jsonFormat7(Standing.apply)

    implicit object ResultsResponseWriter extends JsonWriter[SparkJobHttpResponse] {
      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒ JsString(view)}
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error)}
        obj.body match {
          case Some(SeasonStandingView(c, west, east, latency, _)) ⇒
            JsObject("western conference" -> JsArray(west.map(_.toJson)),
                     "eastern conference" -> JsArray(east.map(_.toJson)),
                     "url" -> url, "view" -> v, "latency" -> JsNumber(latency),
                     "body" -> JsObject("count" -> JsNumber(c)),
                     "error" -> error)
          case Some(PlayoffStandingView(c, table, latency, _)) ⇒
            JsObject("playoff" -> JsArray(table.map(_.toJson)),
                     "url" -> url, "view" -> v,
                     "latency" -> JsNumber(latency),
                     "body" -> JsObject("count" -> JsNumber(c)),
                     "error" -> error)
          case Some(FilteredView(c, results, latency, _)) ⇒
            JsObject("url" -> url,
                     "view" -> JsArray(results.map(_.toJson)),
                     "latency" -> JsNumber(latency),
                     "body" -> JsObject("count" -> JsNumber(c)),
                     "error" -> error)
          case Some(FilteredAllView(c, results, latency, _)) ⇒
            JsObject("url" -> url,
                     "view" -> JsArray(results.map(_.toJson)),
                     "latency" -> JsNumber(latency),
                     "body" -> JsObject("count" -> JsNumber(c)),
                     "error" -> error)
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }
  }
}

import javax.ws.rs.Path
import io.swagger.annotations._
import scala.concurrent.duration._

@io.swagger.annotations.Api(value = "standings", produces = "application/json")
@Path("/api/standing/")
class StandingRouter(override val host: String, override val httpPort: Int,
                     override val intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
                     override val teams: scala.collection.mutable.HashMap[String, String],
                     override val httpPrefixAddress: String = "standing",
                     arenas: scala.collection.immutable.Vector[(String, String)], context: SparkContext)
                     (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport with TypedAsk with ParamsValidation with StandingHttpProtocols {
  private val querySupervisor = system.actorOf(SparkQuerySupervisor.props)
  override implicit val timeout = akka.util.Timeout(10.seconds)

  val route = standingRoute()

  @Path("/{stage}")
  @ApiOperation(value = "Search rebounds leaders by stage", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "Authorization", value = "Authorization token", required = true, dataType = "string", paramType = "header"),
    new ApiImplicitParam(name = "stage", value = "Stage of results. Examples: season-14-15,playoff-15-16", required = true, dataType = "string", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[PtsLeadersView]),
    new ApiResponse(code = 403, message = "The supplied authentication is not authorized to access this resource"),
    new ApiResponse(code = 404, message = "Unsupported season or team"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def standingRoute(): Route =
    pathPrefix(pathPrefix) {
      (get & path(httpPrefixAddress / Segment)) { stage ⇒
        withUri { url ⇒
          requiredHttpSession(ec) { session ⇒
            system.log.info(s"[user:${session.user}] accesses $url")
            get(complete(searchResults(url, stage)))
          }
        }
      }
    }

  private def searchResults(url: String, stage: String): Future[HttpResponse] = {
    validatePeriod(stage).fold({ error => Future.successful(notFound(s"Invalid parameters: $error")) }, { _ =>
      fetch[SparkQueryView](StandingQueryArgs(context, url, stage, teams, stage), querySupervisor).map {
        case cats.data.Xor.Right(res) ⇒ success(SparkJobHttpResponse(url, view = Option("standing"), body = Option(res), error = res.error))
        case cats.data.Xor.Left(ex) ⇒ internalError(ex)
      }
    })
  }
}

