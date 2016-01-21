import java.net.{ NetworkInterface, InetAddress }
import java.util.Date

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{ Route, Directive1, Directives }
import akka.stream.ActorMaterializer
import ingestion.JournalChangesIngestion
import org.apache.spark.SparkContext
import spark.SparkQuery
import akka.pattern.AskTimeoutException
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.joda.time.{ DateTime, Interval }

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag
import scalaz.{ -\/, \/-, \/ }
import com.github.nscala_time.time.Imports._

package object http {

  private val HttpDispatcher = "akka.http-dispatcher"
  private[http] val SparkDispatcher = "akka.spark-dispatcher"
  private[http] val ActorSystemName = "SportCenter"
  private[http] val DefaultHttpPort = 8012
  private[http] val SEEDS_ENV = "SEED_NODES"
  private[http] val HTTP_PORT = "HTTP_PORT"
  private[http] val NET_INTERFACE = "NET_INTERFACE"

  case class NbaResultView(homeTeam: String, homeScore: Int, awayTeam: String, awayScore: Int, dt: Date)
  case class ResultAdded(team: String, r: NbaResult)
  case class NbaResult(homeTeam: String, homeScore: Int, awayTeam: String, awayScore: Int, dt: Date,
                       homeScoreBox: String = "", awayScoreBox: String = "",
                       homeTotal: Total = Total(), awayTotal: Total = Total(),
                       homeBox: List[PlayerLine] = Nil, awayBox: List[PlayerLine] = Nil)

  case class Total(min: Int = 0, fgmA: String = "", threePmA: String = "", ftmA: String = "", minusSlashPlus: String = "",
                   offReb: Int = 0, defReb: Int = 0, totalReb: Int = 0, ast: Int = 0, pf: Int = 0,
                   steels: Int = 0, to: Int = 0, bs: Int = 0, ba: Int = 0, pts: Int = 0)

  case class PlayerLine(name: String = "", pos: String = "", min: String = "", fgmA: String = "",
                        threePmA: String = "", ftmA: String = "", minusSlashPlus: String = "",
                        offReb: Int = 0, defReb: Int = 0, totalReb: Int = 0, ast: Int = 0, pf: Int = 0, steels: Int = 0,
                        to: Int = 0, bs: Int = 0, ba: Int = 0, pts: Int = 0)

  trait DefaultJobArgs {
    def url: String
    def ctx: SparkContext
  }

  trait DefaultHttpResponse {
    def url: String
    def view: Option[String]
    def error: Option[String]
    def body: Option[DefaultResponseBody]
  }

  trait DefaultResponseBody {
    def count: Int
  }

  private[http] trait TypedAsk {
    import akka.pattern.ask
    def fetch[T <: DefaultResponseBody](message: DefaultJobArgs, target: ActorRef)(implicit ec: ExecutionContext, fetchTimeout: akka.util.Timeout, tag: ClassTag[T]): Future[String \/ T] =
      target
        .ask(message)
        .mapTo[T]
        .map(\/-(_))
        .recoverWith {
          case ex: ClassCastException  ⇒ Future.successful(-\/(ex.getMessage))
          case ex: AskTimeoutException ⇒ Future.successful(-\/(s"Fetch results operation timeout ${ex.getMessage}"))
        }
  }

  private[http] case class Api(route: Option[ExecutionContext ⇒ Route] = None,
                               preAction: Option[() ⇒ Unit] = None,
                               postAction: Option[() ⇒ Unit] = None,
                               urls: String = "") extends Directives {

    private def cmbRoutes(r0: ExecutionContext ⇒ Route, r1: ExecutionContext ⇒ Route) =
      (ec: ExecutionContext) ⇒
        r0(ec) ~ r1(ec)

    private def cmbActions(a1: () ⇒ Unit, a2: () ⇒ Unit) =
      () ⇒ {
        a1()
        a2()
      }

    def compose(that: Api): Api =
      Api((route ++ that.route).reduceOption(cmbRoutes),
        (preAction ++ that.preAction).reduceOption(cmbActions),
        (postAction ++ that.postAction).reduceOption(cmbActions),
        s"$urls\n${that.urls}")

    def ~(that: Api): Api = compose(that)
  }

  private[http] trait RestInstaller {

    def context: SparkContext

    protected val pathPrefix = "api"

    protected def configureApi() = Api()

    protected def httpPrefixAddress: String

    protected val httpDispatcher = HttpDispatcher

    protected def installApi(api: Api, interface: String, httpPort: Int)(implicit materializer: akka.stream.ActorMaterializer, system: ActorSystem) = {
      api.route.foreach { api ⇒
        implicit val ec = system.dispatchers.lookup(httpDispatcher)
        val route = api(ec)

        system.registerOnTermination { system.log.info("Http server was stopped") }
        Http().bindAndHandle(akka.http.scaladsl.server.RouteResult.route2HandlerFlow(route), interface, httpPort)
      }

      api.preAction.foreach(action ⇒ action())
    }

    protected def uninstallApi(api: Api) = api.postAction.foreach(action ⇒ action())
  }

  private[http] trait DefaultRestMicroservice extends RestInstaller with Directives { mixin: MicroKernel ⇒
    import spray.json._
    import akka.http.scaladsl.model._

    implicit def timeout: akka.util.Timeout

    def withUri: Directive1[String] = extract(_.request.uri.toString())

    protected def fail[T <: DefaultJobArgs](resp: T)(implicit writer: JsonWriter[T]): String ⇒ Future[HttpResponse] =
      error ⇒
        Future.successful(
          HttpResponse(StatusCodes.BadRequest, scala.collection.immutable.Seq[HttpHeader](),
            HttpEntity(ContentTypes.`application/json`, ByteString(resp.toJson.prettyPrint))))

    protected def fail(error: String) =
      HttpResponse(StatusCodes.InternalServerError, entity = error)

    protected def success[T <: DefaultHttpResponse](resp: T)(implicit writer: JsonWriter[T]) =
      HttpResponse(StatusCodes.OK, scala.collection.immutable.Seq[HttpHeader](),
        HttpEntity(ContentTypes.`application/json`, ByteString(resp.toJson.prettyPrint)))
  }

  private[http] trait ClusterNetwork {

    def ethName: String

    def localAddress: String

    def externalAddress: String

    def domain: String

    def httpPort: Int

    def httpPrefixAddress = s"http://$externalAddress:$httpPort"
  }

  private[http] trait AddressResolver {
    mixin: ClusterNetwork ⇒
    import scala.collection.JavaConverters._

    protected val ipExpression = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"""

    def addresses: Option[InetAddress] =
      NetworkInterface.getNetworkInterfaces.asScala.toList
        .find(_.getName == ethName)
        .flatMap(x ⇒ x.getInetAddresses.asScala.toList.find(i ⇒ i.getHostAddress.matches(ipExpression)))
  }

  case class ServerSession(user: String, password: String)

  private[http] trait BootableMicroservice {
    def system: ActorSystem
    def environment: String
    def startup(): Unit
    def shutdown(): Unit
    def intervals: mutable.LinkedHashMap[Interval, String]
    def teams: mutable.HashMap[String, String]
    def arenas: Seq[(String, String)]
  }

  private[http] abstract class MicroKernel(override val httpPort: Int = DefaultHttpPort,
                                           override val ethName: String) extends BootableMicroservice
      with RestInstaller with ClusterNetwork with AddressResolver {

    lazy val api = configureApi()
    override lazy val system = ActorSystem(ActorSystemName, config)

    override val domain = "haghard.com"

    override def localAddress = addresses.map(_.getHostAddress).getOrElse("0.0.0.0")

    override def externalAddress = System.getenv("HOST")

    override val teams = {
      asScalaBuffer(system.settings.config
        .getConfig("app-settings")
        .getObjectList("teams"))
        ./:(scala.collection.mutable.HashMap[String, String]()) { (acc, c) ⇒
          val it = c.entrySet().iterator()
          if (it.hasNext) {
            val entry = it.next()
            acc += (entry.getKey -> entry.getValue.render().replace("\"", ""))
          }
          acc
        }
    }

    override val arenas = {
      asScalaBuffer(system.settings.config
        .getConfig("app-settings")
        .getObjectList("arenas"))
        ./:(scala.collection.immutable.Vector.empty[(String, String)]) { (acc, c) ⇒
          val it = c.entrySet().iterator()
          if (it.hasNext) {
            val entry = it.next()
            acc :+ (entry.getKey -> entry.getValue.render().replace("\"", ""))
          } else acc
        }
    }

    override val intervals = {
      var views = scala.collection.mutable.LinkedHashMap[Interval, String]()
      val timeZone = cassandra.SCENTER_TIME_ZONE
      var start: Option[DateTime] = None
      var end: Option[DateTime] = None
      var period: Option[String] = None

      val stages = system.settings.config.getConfig("app-settings")
        .getObjectList("stages")
        ./:(scala.collection.mutable.LinkedHashMap[String, String]()) { (acc, c) ⇒
          val it = c.entrySet().iterator()
          if (it.hasNext) {
            val entry = it.next()
            acc += (entry.getKey -> entry.getValue.render().replace("\"", ""))
          }
          acc
        }

      for ((k, v) ← stages) {
        if (start.isEmpty) {
          start = Some(new DateTime(v).withZone(timeZone).withTime(23, 59, 59, 0))
          period = Some(k)
        } else {
          end = Some(new DateTime(v).withZone(timeZone).withTime(23, 59, 58, 0))
          val interval = (start.get to end.get)
          views = views += (interval -> period.get)
          start = Some(end.get.withTime(23, 59, 59, 0))
          period = Some(k)
        }
      }
      views
    }

    lazy val config = ConfigFactory.load("application")

    lazy val context = new SparkQuery {
      override val name = ""
    }.createSparkContext(config, config.getString("db.cassandra.seeds").split(",")(0))

    override def startup(): Unit = {
      system
      val message = new StringBuilder().append('\n')
        .append("=====================================================================================================================================")
        .append('\n')
        .append(s"★ ★ ★ ★ ★ ★  Web service env: $environment [ext: $externalAddress - docker: $localAddress] ★ ★ ★ ★ ★ ★")
        .append('\n')
        .append(s"★ ★ ★ ★ ★ ★  Cassandra contact points: ${system.settings.config.getString("db.cassandra.seeds")}  ★ ★ ★ ★ ★ ★")
        .append('\n')
        .append(s"★ ★ ★ ★ ★ ★  Available urls: ${api.urls}")
        .append('\n')
        .append("=====================================================================================================================================")
        .append('\n')
        .toString

      system.log.info(message)

      val settings = akka.stream.ActorMaterializerSettings(system)
        .withInputBuffer(32, 64)
        .withDispatcher("akka.stream-dispatcher")
      implicit val mat = ActorMaterializer(settings)(system)

      installApi(api, externalAddress, httpPort)(mat, system)

      //Streaming
      JournalChangesIngestion.start(context, config, teams)
    }

    override def shutdown() = {
      uninstallApi(api)
      context.stop
      system.log.info("Web service has been stopped")
    }
  }
}