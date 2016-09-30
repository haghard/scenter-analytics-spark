import java.net.{InetAddress, NetworkInterface}
import java.util.Date

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{Directive1, Directives, RouteConcatenation}
import akka.pattern.AskTimeoutException
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import http.swagger.CorsSupport
import ingestion.JournalChangesIngestion
import org.apache.spark.SparkContext
import org.joda.time.{DateTime, Interval}
import spark.SparkQuery

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

//import scalaz.{-\/, \/-, \/}
import com.github.nscala_time.time.Imports._

package object http {
  //import scalaz.concurrent.{Task => ZTask}
  //import scala.concurrent.{ExecutionContext, Future => SFuture, Promise}

  //Integration code between Scalaz and Scala standard concurrency libraries
  /*
  object Task2Future {

    def fromScala[A](future: SFuture[A])(implicit ec: ExecutionContext): ZTask[A] =
      scalaz.concurrent.Task.async(handlerConversion andThen future.onComplete)

    def fromScalaDeferred[A](future: => SFuture[A])(
        implicit ec: ExecutionContext): ZTask[A] =
      scalaz.concurrent.Task.delay(fromScala(future)(ec)).flatMap(identity)

    def unsafeToScala[A](task: ZTask[A]): SFuture[A] = {
      val p = Promise[A]
      task.runAsync { _ fold (p failure _, p success _) }
      p.future
    }

    private def handlerConversion[A]: ((Throwable \/ A) => Unit) => Try[A] => Unit =
      callback => { t: Try[A] => \/.fromTryCatchNonFatal(t.get) } andThen callback
  }*/

  private val HttpDispatcher = "akka.http-dispatcher"
  private[http] val SparkDispatcher = "akka.spark-dispatcher"
  private[http] val ActorSystemName = "SportCenter"
  private[http] val DefaultHttpPort = 8012
  private[http] val SEEDS_ENV = "SEED_NODES"
  private[http] val HTTP_PORT = "HTTP_PORT"
  private[http] val NET_INTERFACE = "NET_INTERFACE"
  private[http] val HOST = "HOST"

  case class NbaResultView(homeTeam: String, homeScore: Int, awayTeam: String, awayScore: Int, dt: Date)

  case class ResultAdded(team: String, r: NbaResult)

  case class NbaResult(homeTeam: String, homeScore: Int, awayTeam: String, awayScore: Int, dt: Date,
                       homeScoreBox: String = "", awayScoreBox: String = "", homeTotal: Total = Total(), awayTotal: Total = Total(),
                       homeBox: List[PlayerLine] = Nil, awayBox: List[PlayerLine] = Nil)

  case class Total(min: Int = 0, fgmA: String = "", threePmA: String = "", ftmA: String = "", minusSlashPlus: String = "",
                   offReb: Int = 0, defReb: Int = 0, totalReb: Int = 0, ast: Int = 0, pf: Int = 0, steels: Int = 0,
                   to: Int = 0, bs: Int = 0, ba: Int = 0, pts: Int = 0)

  case class PlayerLine(name: String = "", pos: String = "", min: String = "", fgmA: String = "",
                        threePmA: String = "", ftmA: String = "", minusSlashPlus: String = "",
                        offReb: Int = 0, defReb: Int = 0, totalReb: Int = 0, ast: Int = 0, pf: Int = 0,
                        steels: Int = 0, to: Int = 0, bs: Int = 0, ba: Int = 0, pts: Int = 0)

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

  trait TypedAsk {

    import akka.pattern.ask

    def fetch[T <: DefaultResponseBody](message: DefaultJobArgs, target: ActorRef)(implicit ec: ExecutionContext, fetchTimeout: akka.util.Timeout, tag: ClassTag[T]): Future[cats.data.Xor[String, T]] =
      target.ask(message).mapTo[T].map(cats.data.Xor.right(_))
        .recoverWith {
          case ex: ClassCastException ⇒ Future.successful(cats.data.Xor.left(s"Class cast error: ${ex.getMessage}"))
          case ex: AskTimeoutException ⇒ Future.successful(cats.data.Xor.left(s"Request timeout: ${ex.getMessage}"))
          case scala.util.control.NonFatal(e) => //Doesn't swallow stack overflow out of memory errors
            Future.successful(cats.data.Xor.left(s"Unexpected error: ${e.getMessage}"))
        }
  }

  trait EndpointInstaller {
    protected val pathPrefix = "api"

    protected def httpPrefixAddress: String

    protected val httpDispatcher = HttpDispatcher

    protected def installApi(context: SparkContext,
                             intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String],
                             arenas: scala.collection.immutable.Vector[(String, String)],
                             teams: scala.collection.mutable.HashMap[String, String],
                             interface: String, httpPort: Int
                            )(implicit materializer: akka.stream.ActorMaterializer, system: ActorSystem) = {

      implicit val _ = system.dispatchers.lookup(httpDispatcher)

      import RouteConcatenation._
      val route = new LoginRouter(interface, httpPort).route ~
        new ResultsRouter(interface, httpPort, intervals, teams, arenas = arenas, context = context).route ~
        new DailyResultsRouter(interface, httpPort, context, intervals, arenas, teams).route ~
        new PlayerStatRouter(interface, httpPort, intervals, teams, arenas = arenas, context = context).route ~
        new PtsLeadersRouter(interface, httpPort, intervals, teams, arenas = arenas, context = context).route ~
        new PtsLeadersRouter(interface, httpPort, intervals, teams, arenas = arenas, context = context).route ~
        new SwaggerDocRouter(interface, httpPort).route

      system.registerOnTermination {
        system.log.info("Http server was stopped")
      }
      Http().bindAndHandle(akka.http.scaladsl.server.RouteResult.route2HandlerFlow(route), interface, httpPort)
        .onComplete {
          case Success(binding) =>
          case Failure(ex) =>
            system.log.error(ex.getMessage)
            ex.printStackTrace()
            sys.exit(-1)
        }
    }
  }

  trait DefaultRestMicroservice extends EndpointInstaller with Directives with CorsSupport {

    import akka.http.scaladsl.model._
    import spray.json._

    implicit def timeout: akka.util.Timeout

    def withUri: Directive1[String] = extract(_.request.uri.toString())

    def internalError[T <: DefaultJobArgs](resp: T)(implicit writer: JsonWriter[T]): String ⇒ Future[HttpResponse] =
      error ⇒
        Future.successful(HttpResponse(StatusCodes.BadRequest, scala.collection.immutable.Seq[HttpHeader](),
          HttpEntity(ContentTypes.`application/json`, ByteString(resp.toJson.prettyPrint))))

    def internalError(error: String) = HttpResponse(StatusCodes.InternalServerError, entity = error)

    def notFound(error: String) = HttpResponse(StatusCodes.NotFound, entity = error)

    def success[T <: DefaultHttpResponse](resp: T)(implicit writer: JsonWriter[T]) =
      HttpResponse(StatusCodes.OK, scala.collection.immutable.Seq[HttpHeader](),
        HttpEntity(ContentTypes.`application/json`, ByteString(resp.toJson.prettyPrint)))
  }

  trait ClusterNetwork {
    def httpPort: Int

    def domain: String

    def ethName: String

    def localAddress: String

    def httpPrefixAddress = s"http://$domain:$httpPort"
  }

  trait NetworkResolver {
    mixin: ClusterNetwork ⇒

    import scala.collection.JavaConverters._

    protected val ipExpression = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"""

    def addresses: Option[InetAddress] =
      NetworkInterface.getNetworkInterfaces.asScala.toList.find(_.getName == ethName)
        .flatMap(x ⇒ x.getInetAddresses.asScala.toList.find(i ⇒ i.getHostAddress.matches(ipExpression)))
  }

  case class ServerSession(user: String, password: String)

  trait BootableMicroservice {
    def system: ActorSystem

    def environment: String

    def startup(): Unit

    def shutdown(): Unit

    def intervals: mutable.LinkedHashMap[Interval, String]

    def teams: mutable.HashMap[String, String]

    def arenas: Seq[(String, String)]
  }

  abstract class MicroKernel(override val httpPort: Int = DefaultHttpPort, override val ethName: String) extends BootableMicroservice
    with EndpointInstaller with ClusterNetwork with NetworkResolver {

    override lazy val system = ActorSystem(ActorSystemName, config)

    override def localAddress = addresses.map(_.getHostAddress).getOrElse("0.0.0.0")

    override val domain = Option(System.getProperty(HOST))
      .fold(throw new Exception(s"$HOST ENV variable should be defined"))(identity)

    override val teams = asScalaBuffer(system.settings.config.getConfig("app-settings")
      .getObjectList("teams"))./:(scala.collection.mutable.HashMap[String, String]()) { (acc, c) ⇒
      val it = c.entrySet().iterator()
      if (it.hasNext) {
        val entry = it.next()
        acc += (entry.getKey -> entry.getValue.render().replace("\"", ""))
      }
      acc
    }

    override val arenas = asScalaBuffer(system.settings.config.getConfig("app-settings")
      .getObjectList("arenas"))./:(scala.collection.immutable.Vector.empty[(String, String)]) { (acc, c) ⇒
      val it = c.entrySet().iterator()
      if (it.hasNext) {
        val entry = it.next()
        acc :+ (entry.getKey -> entry.getValue.render().replace("\"", ""))
      } else acc
    }

    override val intervals = {
      var views = scala.collection.mutable.LinkedHashMap[Interval, String]()
      val timeZone = cassandra.SCENTER_TIME_ZONE
      var start: Option[DateTime] = None
      var end: Option[DateTime] = None
      var period: Option[String] = None

      val stages = system.settings.config.getConfig("app-settings").getObjectList("stages")
        ./:(scala.collection.mutable.LinkedHashMap[String, String]()) {
          (acc, c) ⇒
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
      override val name = "scenter-analytics"
    }.createSparkContext(config, config.getString("db.cassandra.seeds"))

    override def startup(): Unit = {
      system
      val message = new StringBuilder()
        .append('\n')
        .append("=====================================================================================================================================")
        .append('\n')
        .append(s"★ ★ ★ ★ ★ ★  Web service env: $environment [ext: $domain - docker: $localAddress] ★ ★ ★ ★ ★ ★")
        .append('\n')
        .append(s"★ ★ ★ ★ ★ ★  Cassandra contact points: ${system.settings.config.getString("db.cassandra.seeds")}  ★ ★ ★ ★ ★ ★")
        .append('\n')
        //.append(s"★ ★ ★ ★ ★ ★  Available urls: ${api.urls}")
        .append('\n')
        .append("=====================================================================================================================================")
        .append('\n')
        .toString

      system.log.info(message)

      val settings = akka.stream.ActorMaterializerSettings(system).withInputBuffer(32, 64).withDispatcher("akka.stream-dispatcher")
      implicit val mat = ActorMaterializer(settings)(system)

      installApi(context, intervals, arenas, teams,
        localAddress, httpPort)(mat, system)

      //Streaming
      JournalChangesIngestion.start(context, config, teams)
    }

    override def shutdown() = {
      //uninstallApi(api)
      context.stop
      system.log.info("Web service has been stopped")
    }
  }

  import http.ResultsRouter.TeamsHttpProtocols
  import http.SparkJob._
  import spray.json.JsonWriter

  case class SparkJobHttpResponse(url: String, view: Option[String] = None,
                                  body: Option[SparkQueryView] = None, error: Option[String] = None) extends DefaultHttpResponse

  trait StandingHttpProtocols extends TeamsHttpProtocols {
    implicit val standingFormat = jsonFormat7(Standing.apply)

    implicit object ResultsResponseWriter extends JsonWriter[SparkJobHttpResponse] {

      import spray.json._

      override def write(obj: SparkJobHttpResponse): spray.json.JsValue = {
        val url = JsString(obj.url.toString)
        val v = obj.view.fold(JsString("none")) {
          JsString(_)
        }
        val error = obj.error.fold(JsString("none")) {
          JsString(_)
        }
        obj.body match {
          case Some(SeasonStandingView(c, west, east, latency, _)) ⇒
            JsObject(
              "western conference" -> JsArray(west.map(_.toJson)),
              "eastern conference" -> JsArray(east.map(_.toJson)),
              "url" -> url, "view" -> v, "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error
            )
          case Some(PlayoffStandingView(c, table, latency, _)) ⇒
            JsObject(
              "playoff" -> JsArray(table.map(_.toJson)),
              "url" -> url, "view" -> v, "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error
            )
          case Some(FilteredView(c, results, latency, _)) ⇒
            JsObject(
              "url" -> url,
              "view" -> JsArray(results.map(_.toJson)),
              "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error
            )
          case Some(FilteredAllView(c, results, latency, _)) ⇒
            JsObject(
              "url" -> url,
              "view" -> JsArray(results.map(_.toJson)),
              "latency" -> JsNumber(latency),
              "body" -> JsObject("count" -> JsNumber(c)),
              "error" -> error
            )
          case None ⇒ JsObject("url" -> url, "view" -> v, "error" -> error)
        }
      }
    }
  }
}
