package http

import scala.util.Try
import scala.collection._
import scala.concurrent.duration._

object Bootstrap extends App {
  val opt = """--(\S+)=(\S+)""".r
  val DefaultEth = "eth0"

  def argsToProps(args: Array[String]) =
    args.collect { case opt(key, value) ⇒ key -> value }(breakOut)

  def applySystemProperties(args: Array[String]) = {
    for ((key, value) ← argsToProps(args)) {
      println(s"SYSTEM ENVIROMENT: $key - $value")
      System.setProperty(key, value)
    }
  }

  if (args.size > 0)
    applySystemProperties(args)

  val httpP = Try(System.getProperty(HTTP_PORT).toInt).getOrElse(DefaultHttpPort)
  val eth = Option(System.getProperty(NET_INTERFACE)).filter(_ != "null").getOrElse(DefaultEth)

  /*
  import scalaz._, Scalaz._
  import akka.http.scaladsl.server.RouteConcatenation._
  val rest = new http.modules.PlayersModule with http.modules.RestModule {
    override implicit val timeout: akka.util.Timeout = akka.util.Timeout(30 seconds)
    override val system: ActorSystem = ???
    override val context: SparkContext = ???
  }

  val routes = (rest.api1 |@| rest.api2)((a, b) ⇒ (ec: ExecutionContext) ⇒ a(ec) ~ b(ec))
   */

  //https://github.com/akka/akka/blob/master/akka-http-tests/src/test/scala/akka/http/scaladsl/server/directives/SecurityDirectivesSpec.scala

  object SparkAnalytics extends MicroKernel(httpPort = httpP, ethName = eth)  /* with SwaggerRouter*/ {
    //implicit val timeout = akka.util.Timeout(30 seconds)
    override val environment = "Spark"

    /*override lazy val googleApiKey = system.settings.config.getString("google.consumer-key")
    override lazy val googleApiSecret = system.settings.config.getString("google.consumer-secret")
    override lazy val twitterApiKey = system.settings.config.getString("twitter.consumer-key")
    override lazy val twitterApiSecret = system.settings.config.getString("twitter.consumer-secret")
    override lazy val githubApiKey = system.settings.config.getString("github.consumer-key")
    override lazy val githubApiSecret = system.settings.config.getString("github.consumer-secret")*/
  }

  SparkAnalytics.startup

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run = SparkAnalytics.shutdown
  }))
}
