package http

import java.net.URLDecoder

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import http.PlayersRouter.PlayersProtocol
import http.StandingRouter.{ StandingHttpProtocols, SparkJobHttpResponse }
import http.SparkJob.{ Stats, PlayerStatsView, PlayerStatsQueryArgs }
import spray.json._

import scala.concurrent.{ Future, ExecutionContext }
import scalaz.{ -\/, \/- }

object PlayersRouter {

  trait PlayersProtocol extends StandingHttpProtocols {

    implicit val statsFormat = jsonFormat14(Stats.apply)

    implicit object PlayersResponseWriter extends JsonWriter[SparkJobHttpResponse] {
      import spray.json._
      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒ JsString(view) }
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error) }
        obj.body match {
          case Some(PlayerStatsView(c, stats, latency, _)) ⇒
            JsObject(
              "url" -> url,
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

trait PlayersRouter extends LeadersRouter with PlayersProtocol { mixin: MicroKernel ⇒
  lazy val enc = "utf-8"
  private val playerServicePath = "player"
  private val playerJobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ playerRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ [${playerServicePath}-routes] was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/$pathPrefix/$playerServicePath/pts/{stage} 'Cookie:_sessiondata= ...']")

  def playerRoute(implicit ex: ExecutionContext): Route =
    pathPrefix(pathPrefix) {
      path(playerServicePath / "stats") {
        get {
          parameters(('name.as[String]), ('period.as[String]), ('team.as[String])) { (name, period, team) ⇒
            withUri { url ⇒
              requiredHttpSession(ex) { session ⇒
                system.log.info(s"[user:${session.user}] access [$httpPrefixAddress/$pathPrefix/$playerServicePath/stats]")
                get(complete(playerStats(URLDecoder.decode(url, enc), URLDecoder.decode(name, enc), period, team)))
              }
            }
          }
        }
      }
    }

  private def playerStats(url: String, name: String, period: String, team: String)(implicit ex: ExecutionContext): Future[HttpResponse] = {
    system.log.info(s"incoming http GET on $url")
    fetch[PlayerStatsView](PlayerStatsQueryArgs(context, url, name, period, team), playerJobSupervisor).map {
      case \/-(res)   ⇒ success(SparkJobHttpResponse(url, view = Option("player-stats"), body = Option(res), error = res.error))(PlayersResponseWriter)
      case -\/(error) ⇒ fail(error)
    }
  }
}