package http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server._
import http.SparkJob._
import http.StandingRouter.SparkJobHttpResponse
import http.TeamsRouter.TeamsHttpProtocols
import spray.json._

import scala.concurrent.{Future, ExecutionContext}
import scalaz.{-\/, \/-}

object TeamsRouter {

  trait TeamsHttpProtocols extends DefaultJsonProtocol {
    implicit val resFormat = jsonFormat4(ResultView)

    implicit object DateFormatToJson
        extends JsonFormat[java.util.Date]
        with DefaultJsonProtocol {
      import spray.json._
      val formatter = cassandra.extFormatter
      override def read(json: JsValue): java.util.Date =
        formatter.parse(json.convertTo[String])
      override def write(date: java.util.Date) = formatter.format(date).toJson
    }

    implicit object TeamsResponseWriter
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
          case Some(TeamStatsView(c, results, latency, _)) ⇒
            JsObject(
                "url" -> url,
                "view" -> JsArray(results.map(_.toJson)),
                "body" -> JsObject("count" -> JsNumber(c)),
                "latency" -> JsNumber(latency),
                "error" -> error
            )
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }
  }
}

trait TeamsRouter
    extends SecurityRouter
    with TypedAsk
    with TeamsHttpProtocols {
  mixin: MicroKernel ⇒
  private val teamsServicePath = "teams"
  private val jobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒
            resultsRoute(ec)
          },
          postAction = Option(
              () ⇒
                system.log.info(
                    s"\n★ ★ ★ $teamsServicePath-routes was stopped on $httpPrefixAddress ★ ★ ★")),
          urls =
            s"[$httpPrefixAddress/$pathPrefix/$teamsServicePath/{stage}?teams=...,... Authorization:...]")

  private def resultsRoute(implicit ec: ExecutionContext): Route =
    pathPrefix(pathPrefix) {
      (get & path(teamsServicePath / Segment)) { period ⇒
        parameters(('teams.as[String])) { teams ⇒
          requiredHttpSession(ec) { session ⇒
            withUri { url ⇒
              system.log.info(
                  s"[user:${session.user}] access [$httpPrefixAddress/$pathPrefix/$teamsServicePath/{stage}]")
              get(complete(search(url, period, teams)))
            }
          }
        }
      }
    }

  private def search(url: String, stage: String, searchTeams: String)(
      implicit ex: ExecutionContext): Future[HttpResponse] = {
    system.log.info(s"incoming http GET $url")
    val interval = (for { (k, v) ← intervals if (v == stage) } yield
      k).headOption
    interval.fold(
        Future.successful(
            fail(s"Unsupported period has been received $stage"))) {
      interval0 ⇒
        fetch[TeamStatsView](TeamStatQueryArgs(context,
                                               url,
                                               stage,
                                               searchTeams.split(",").toSeq,
                                               arenas,
                                               teams),
                             jobSupervisor).map {
          case \/-(res) ⇒
            success(
                SparkJobHttpResponse(url,
                                     view = Option("team-stats"),
                                     body = Option(res),
                                     error = res.error))
          case -\/(error) ⇒ fail(error)
        }
    }
  }
}
