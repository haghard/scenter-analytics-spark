package http

import javax.ws.rs.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server._
import cats.data.Validated
import http.DailyResultsRouter.{Args, DailyResultsProtocol}
import http.SparkJob._
import io.swagger.annotations._
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object DailyResultsRouter {

  trait DailyResultsProtocol extends StandingHttpProtocols {

    implicit object DailyResultsWriter
      extends JsonWriter[SparkJobHttpResponse] {
      override def write(obj: SparkJobHttpResponse): JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒ JsString(view) }
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error) }
        obj.body match {
          case Some(DailyView(c, results, latency, _)) ⇒
            JsObject("url" -> url, "view" -> JsArray(results.map(_.toJson)),
              "body" -> JsObject("count" -> JsNumber(c)), "latency" -> JsNumber(latency), "error" -> error)
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }

  }

  case class Args(period: String, year: Int, mm: Int, dd: Int)
}

@io.swagger.annotations.Api(value = "/daily", produces = "application/json")
@Path("/api/daily")
class DailyResultsRouter(override val host: String, override val httpPort: Int,
                         context: SparkContext,
                         intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
                         arenas: scala.collection.immutable.Vector[(String, String)],
                         teams: scala.collection.mutable.HashMap[String, String],
                         override val httpPrefixAddress: String = "daily")
                        (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport with TypedAsk with DailyResultsProtocol {
  private val dailyJobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  override implicit val timeout = akka.util.Timeout(10.seconds)

  val route = dailyRoute

  @Path("/{day}")
  @ApiOperation(value = "Fetch results by day", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "day", value = "Day. Examples:2016-01-10, 2015-09-12", required = true, dataType = "string", paramType = "path"),
    new ApiImplicitParam(name = "Authorization", value = "Authorization token", required = true, dataType = "string", paramType = "header")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[DailyView]),
    new ApiResponse(code = 403, message = "The supplied authentication is not authorized to access this resource"),
    new ApiResponse(code = 404, message = "Unsupported season or team"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def dailyRoute(): Route =
    pathPrefix(pathPrefix) {
      (get & path(httpPrefixAddress / Segment)) { day ⇒
        withUri { url ⇒
          requiredHttpSession(ec) { session ⇒
            system.log.info(s"[user:${session.user}] access [$host:$httpPort/$pathPrefix/$httpPrefixAddress/$day]")
            get(complete(searchResults(url, day)))
          }
        }
      }
    }

  private def validatePeriod(yyyyMmdd: (Int, Int, Int)): Validated[String, Args] = {
    (for {
      (interval, stage) ← intervals
      if (interval contains new DateTime(yyyyMmdd._1, yyyyMmdd._2, yyyyMmdd._3, 0, 0).withZone(cassandra.SCENTER_TIME_ZONE))
    } yield Args(stage, yyyyMmdd._1, yyyyMmdd._2, yyyyMmdd._3)).headOption
      .fold(Validated.invalid[String, Args](s"\n Could'n find season for ${yyyyMmdd} day"))(a => Validated.valid[String, Args](a))
  }

  private def parseDay(stage: String): Validated[String, (Int, Int, Int)] =
    try {
      val fields = stage.split("-")
      Validated.valid[String, (Int, Int, Int)](fields(0).toInt, fields(1).toInt, fields(2).toInt)
    } catch {
      case ex: Exception => Validated.invalid[String, (Int, Int, Int)](ex.getMessage)
    }

  private def searchResults(url: String, day: String): Future[HttpResponse] = {
    parseDay(day).andThen(validatePeriod).fold({ error: String => Future.successful(notFound(s"Invalid parameters: $error")) }, { arg =>
      fetch[DailyView](DailyResultsQueryArgs(context, url, arg.period, (arg.year, arg.mm, arg.dd) , arenas, teams), dailyJobSupervisor).map {
        case cats.data.Xor.Right(res) => success(SparkJobHttpResponse(url, view = Option("daily-results"), body = Option(res), error = res.error))(DailyResultsWriter)
        case cats.data.Xor.Left(ex) => internalError(ex)
      }
    })
  }
}