package http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Route
import http.PlayersRouter.PlayersProtocol
import http.SparkJob._
import http.LeadersRouter.LeadersProtocol
import spray.json.JsonWriter
import http.StandingRouter.SparkJobHttpResponse

import scala.concurrent.{ Future, ExecutionContext }
import scala.reflect.ClassTag
import scalaz.{ -\/, \/- }

object LeadersRouter {

  type HttpArgs = JobManagerProtocol with DefaultJobArgs

  trait LeadersProtocol extends PlayersProtocol {
    implicit val ptsLeaderFormat = jsonFormat4(PtsLeader.apply)
    implicit val rebLeaderFormat = jsonFormat6(RebLeader.apply)

    implicit object LeadersResponseWriter extends JsonWriter[SparkJobHttpResponse] {
      import spray.json._
      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒ JsString(view) }
        val error = obj.error.fold(JsString("none")) { error ⇒ JsString(error) }
        obj.body match {
          case Some(RebLeadersView(c, leaders, latency, _)) ⇒
            JsObject(
              "url" -> url,
              "view" -> JsArray(leaders.map(_.toJson)),
              "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error)
          case Some(PtsLeadersView(c, leaders, latency, _)) ⇒
            JsObject(
              "url" -> url,
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

trait LeadersRouter extends StandingRouter with LeadersProtocol { mixin: MicroKernel ⇒
  import LeadersRouter._
  private val leadersServicePath = "leaders"
  private val leadersJobSupervisor = system.actorOf(SparkQuerySupervisor.props)
  private val defaultDepth = 10

  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ leadersRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ [${leadersServicePath}-routes] was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/$pathPrefix/$leadersServicePath/pts/{stage}]\n[$httpPrefixAddress/$pathPrefix/$leadersServicePath/reb/{stage}]")

  private def leadersRoute(implicit ex: ExecutionContext): Route =
    pathPrefix(pathPrefix) {
      (get & path(leadersServicePath / "pts" / Segment)) { stage ⇒
        parameters(('depth.as[Int] ? defaultDepth)) { depth ⇒
          withUri { url ⇒
            requiredHttpSession(ex) { session ⇒
              system.log.info(s"[user:${session.user}] access [$httpPrefixAddress/$pathPrefix/$leadersServicePath/pts]")
              get(complete(searchFor[PtsLeadersView](PtsLeadersSearchArgs(context, url, stage, teams, stage, depth), "pts-leaders")))
            }
          }
        }
      } ~
        (get & path(leadersServicePath / "reb" / Segment)) { period ⇒
          parameters(('depth.as[Int] ? defaultDepth)) { depth ⇒
            withUri { url ⇒
              requiredHttpSession(ex) { session ⇒
                system.log.info(s"[user:${session.user}] access [$httpPrefixAddress/$pathPrefix/$leadersServicePath/reb]")
                get(complete(searchFor[RebLeadersView](RebLeadersSearchArgs(context, url, period, depth), "reb-leaders")))
              }
            }
          }
        }
    }

  def searchFor[T <: SparkQueryView](args: HttpArgs, name: String)(implicit ex: ExecutionContext, tag: ClassTag[T]): Future[HttpResponse] = {
    fetch[T](args, leadersJobSupervisor).map {
      case \/-(res)   ⇒ success(SparkJobHttpResponse(args.url, view = Option(name), body = Option(res), error = res.error))(LeadersResponseWriter)
      case -\/(error) ⇒ fail(error)
    }
  }
}