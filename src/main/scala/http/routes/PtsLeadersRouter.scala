package http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import http.routes.PlayerStatRouter.PlayersProtocol
import org.apache.spark.SparkContext
import spark.SparkJob._
import spark.{SparkJob, SparkQuerySupervisor}
import spray.json.JsonWriter

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object PtsLeadersRouter {
  trait LeadersProtocol extends PlayersProtocol {
    implicit val ptsLeaderFormat = spray.json.DefaultJsonProtocol.jsonFormat4(PtsLeader.apply)
    implicit val rebLeaderFormat = spray.json.DefaultJsonProtocol.jsonFormat6(RebLeader.apply)

    implicit object LeadersResponseWriter extends JsonWriter[SparkJobHttpResponse] {
      import spray.json._
      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒ JsString(view)}
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error)}
        obj.body match {
          case Some(RebLeadersView(c, leaders, latency, _)) ⇒
            JsObject("url" -> url,
                     "view" -> JsArray(leaders.map(_.toJson)),
                     "latency" -> JsNumber(latency),
                     "body" -> JsObject("count" -> JsNumber(c)),
                     "error" -> error)
          case Some(PtsLeadersView(c, leaders, latency, _)) ⇒
            JsObject("url" -> url,
                     "view" -> JsArray(leaders.map(_.toJson)),
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

@io.swagger.annotations.Api(value = "pts leaders", produces = "application/json")
@Path("/api/leaders/pts")
class PtsLeadersRouter(override val host: String, override val httpPort: Int,
                       override val intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
                       override val teams: scala.collection.mutable.HashMap[String, String],
                       override val httpPrefixAddress: String = "leaders",
                       arenas: scala.collection.immutable.Vector[(String, String)], context: SparkContext)
                      (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport with LeadersProtocol with TypedAsk with ParamsValidation {
  private val defaultDepth = 10
  private val jobSupervisor = system.actorOf(SparkQuerySupervisor.props)
  override implicit val timeout = akka.util.Timeout(10.seconds)

  val route = ptsLeadersRoute()

  @Path("/{stage}")
  @ApiOperation(value = "Search pts leaders by stage", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "Authorization", value = "Authorization token", required = true, dataType = "string", paramType = "header"),
    new ApiImplicitParam(name = "stage", value = "Stage of results. Examples: season-14-15,playoff-15-16", required = true, dataType = "string", paramType = "path"),
    new ApiImplicitParam(name = "depth", value = "Depth of results", required = false, dataType = "int", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[PtsLeadersView]),
    new ApiResponse(code = 403, message = "The supplied authentication is not authorized to access this resource"),
    new ApiResponse(code = 404, message = "Unsupported season or team"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def ptsLeadersRoute(): Route =
    pathPrefix(pathPrefix) {
      (get & path(httpPrefixAddress / "pts" / Segment)) { stage ⇒
        parameters(('depth.as[Int] ? defaultDepth)) { depth ⇒
          withUri { url ⇒
            requiredHttpSession(ec) { session ⇒
              system.log.info(s"[user:${session.user}] accesses $url")
              get(complete(searchPts(url, stage, depth)))
            }
          }
        }
      }
    }

  private def searchPts(url: String, stage: String, depth: Int): Future[HttpResponse] = {
    validatePeriod(stage).fold({ error => Future.successful(notFound(s"Invalid parameters: $error")) }, { _ =>
      fetch[PtsLeadersView](PtsLeadersQueryArgs(context, url, teams, stage, depth), jobSupervisor).map {
        case cats.data.Xor.Right(res) ⇒ success(SparkJobHttpResponse(url, view = Option("pts-leaders"),
          body = Option(res), error = res.error))(LeadersResponseWriter)
        case cats.data.Xor.Left(ex) ⇒ internalError(ex)
      }
    })
  }
}