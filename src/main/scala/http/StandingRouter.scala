package http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import http.SparkJob._
import http.TeamsRouter.TeamsHttpProtocols
import spray.json.JsonWriter
import http.StandingRouter.{ StandingHttpProtocols, SparkJobHttpResponse }

import scala.concurrent.{ Future, ExecutionContext }
import scalaz.{ -\/, \/- }

object StandingRouter {

  case class SparkJobHttpResponse(url: String, view: Option[String] = None,
                                  body: Option[SparkQueryView] = None,
                                  error: Option[String] = None) extends DefaultHttpResponse

  trait StandingHttpProtocols extends TeamsHttpProtocols {
    implicit val standingFormat = jsonFormat7(Standing.apply)

    implicit object ResultsResponseWriter extends JsonWriter[SparkJobHttpResponse] {
      import spray.json._
      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒ JsString(view) }
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error) }
        obj.body match {
          case Some(SeasonStandingView(c, west, east, latency, _)) ⇒
            JsObject("western conference" -> JsArray(west.map(_.toJson)),
              "eastern conference" -> JsArray(east.map(_.toJson)),
              "url" -> url,
              "view" -> v,
              "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error)
          case Some(PlayoffStandingView(c, table, latency, _)) ⇒
            JsObject("playoff" -> JsArray(table.map(_.toJson)),
              "url" -> url,
              "view" -> v,
              "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error)
          case Some(FilteredView(c, results, latency, _)) ⇒
            JsObject(
              "url" -> url,
              "view" -> JsArray(results.map(_.toJson)),
              "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error)
          case Some(FilteredAllView(c, results, latency, _)) ⇒
            JsObject(
              "url" -> url,
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

trait StandingRouter extends TeamsRouter with TypedAsk with StandingHttpProtocols { mixin: MicroKernel ⇒
  private val standingServicePath = "standing"
  private val querySupervisor = system.actorOf(SparkQuerySupervisor.props)

  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ resultsRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ $standingServicePath-routes was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/$pathPrefix/$standingServicePath/{stage}]")

  private def resultsRoute(implicit ec: ExecutionContext): Route =
    pathPrefix(pathPrefix) {
      (get & path(standingServicePath / Segment)) { stage ⇒
        withUri { url ⇒
          requiredHttpSession(ec) { session ⇒
            system.log.info(s"[user:${session.user}] access [$httpPrefixAddress/$pathPrefix/$standingServicePath/$stage]")
            get(complete(search(url, stage)))
          }
        }
      }
    }

  private def search(url: String, stage: String)(implicit ex: ExecutionContext): Future[HttpResponse] = {
    system.log.info(s"incoming http GET $url")
    val interval = (for { (k, v) ← intervals if (v == stage) } yield k).headOption
    interval.fold(Future.successful(fail(s"Unsupported period has been received $stage"))) { interval0 ⇒
      fetch[SparkQueryView](StandingSearchArgs(context, url, stage, teams, stage), querySupervisor).map {
        case \/-(res)   ⇒ success(SparkJobHttpResponse(url, view = Option("standing"), body = Option(res), error = res.error))
        case -\/(error) ⇒ fail(error)
      }
    }
  }
}