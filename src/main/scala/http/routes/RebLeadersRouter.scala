package http.routes

import javax.ws.rs.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server._
import http.PtsLeadersRouter.LeadersProtocol
import spark.{SparkQuerySupervisor, SparkJob}
import SparkJob.{PtsLeadersView, RebLeadersQueryArgs, RebLeadersView}
import io.swagger.annotations._
import org.apache.spark.SparkContext

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

@io.swagger.annotations.Api(value = "rebounds leaders", produces = "application/json")
@Path("/api/leaders/reb")
class RebLeadersRouter(override val host: String, override val httpPort: Int,
                       override val intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
                       override val teams: scala.collection.mutable.HashMap[String, String],
                       override val httpPrefixAddress: String = "leaders",
                       arenas: scala.collection.immutable.Vector[(String, String)], context: SparkContext)
                      (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport with LeadersProtocol with TypedAsk with ParamsValidation {
  private val defaultDepth = 10
  private val jobSupervisor = system.actorOf(SparkQuerySupervisor.props)
  override implicit val timeout = akka.util.Timeout(10.seconds)

  val route = rebLeadersRoute()

  @Path("/{stage}")
  @ApiOperation(value = "Search rebounds leaders by stage", notes = "", httpMethod = "GET")
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
  def rebLeadersRoute(): Route =
    pathPrefix(pathPrefix) {
      (get & path(httpPrefixAddress / "reb" / Segment)) { stage ⇒
        parameters(('depth.as[Int] ? defaultDepth)) { depth ⇒
          withUri { url ⇒
            requiredHttpSession(ec) { session ⇒
              system.log.info(s"[user:${session.user}] accesses $url")
              get(complete(searchReb(url, stage, depth)))
            }
          }
        }
      }
    }

  private def searchReb(url: String, stage: String, depth: Int): Future[HttpResponse] = {
    validatePeriod(stage).fold({ error => Future.successful(notFound(s"Invalid parameters: $error")) }, { _ =>
      fetch[RebLeadersView](RebLeadersQueryArgs(context, url, stage, depth), jobSupervisor).map {
        case cats.data.Xor.Right(res) ⇒ success(SparkJobHttpResponse(url, view = Option("reb-leaders"),
          body = Option(res), error = res.error))(LeadersResponseWriter)
        case cats.data.Xor.Left(ex) ⇒ internalError(ex)
      }
    })
  }
}