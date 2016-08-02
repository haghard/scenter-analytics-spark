package http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server._
import http.DailyResultsRouter.{Args, DailyResultsProtocol}
import http.SparkJob._
import http.StandingRouter.{SparkJobHttpResponse, StandingHttpProtocols}
import org.joda.time.DateTime
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object DailyResultsRouter {

  trait DailyResultsProtocol extends StandingHttpProtocols {

    implicit object DailyResultsWriter
        extends JsonWriter[SparkJobHttpResponse] {
      override def write(obj: SparkJobHttpResponse): JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) { view ⇒
          JsString(view)
        }
        val error = obj.error.fold(JsString("none")) { error ⇒
          JsString(error)
        }
        obj.body match {
          case Some(DailyView(c, results, latency, _)) ⇒
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

  case class Args(period: String, year: Int, mm: Int, dd: Int)

}

trait DailyResultsRouter extends PlayersRouter with DailyResultsProtocol {
  mixin: MicroKernel ⇒

  private val dailyResultsPath = "daily"
  private val dailyJobSupervisor = system.actorOf(SparkQuerySupervisor.props)

  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒
            daily(ec)
          },
          postAction = Option(
              () ⇒
                system.log.info(
                    s"\n★ ★ ★ [${dailyResultsPath}-routes] was stopped on $httpPrefixAddress ★ ★ ★")),
          urls =
            s"[$httpPrefixAddress/$pathPrefix/$dailyResultsPath/{date} Authorization:...']")

  def daily(implicit ex: ExecutionContext): Route =
    pathPrefix(pathPrefix) {
      (get & path(dailyResultsPath / Segment)) { stage ⇒
        withUri { url ⇒
          requiredHttpSession(ex) { session ⇒
            system.log.info(
                s"[user:${session.user}] access [$httpPrefixAddress/$pathPrefix/$dailyResultsPath/$stage]")
            get(complete(searchResults(url, stage)))
          }
        }
      }
    }

  def parseStage(stage: String): Option[(Int, Int, Int)] =
    Try {
      val fields = stage.split("-")
      Option((fields(0).toInt, fields(1).toInt, fields(2).toInt))
    }.getOrElse(None)

  def findPeriod(yyyyMmdd: (Int, Int, Int)) =
    (for {
      (interval, v) ← intervals
      if (interval contains new DateTime(
              yyyyMmdd._1,
              yyyyMmdd._2,
              yyyyMmdd._3,
              0,
              0).withZone(cassandra.SCENTER_TIME_ZONE))
    } yield v).headOption

  private def searchResults(url: String, stage: String)(
      implicit ex: ExecutionContext): Future[HttpResponse] = {
    import scalaz._, Scalaz._
    system.log.info(s"incoming http GET on $url")
    val args: Option[Args] = for {
      dt ← parseStage(stage)
      per ← findPeriod(dt)
    } yield Args(per, dt._1, dt._2, dt._3)

    args.fold(Future.successful(fail(s"Period error $stage"))) { args: Args ⇒
      fetch[DailyView](DailyResultsQueryArgs(context,
                                             url,
                                             args.period,
                                             (args.year, args.mm, args.dd),
                                             arenas,
                                             teams),
                       dailyJobSupervisor).map {
        case \/-(res) ⇒
          success(
              SparkJobHttpResponse(url,
                                   view = Option("daily-results"),
                                   body = Option(res),
                                   error = res.error))(DailyResultsWriter)
        case -\/(error) ⇒ fail(error)
      }
    }
  }
}
