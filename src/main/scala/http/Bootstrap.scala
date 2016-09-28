package http

import scala.util.Try
import scala.collection._

object Bootstrap extends App {
  val opt = """(\S+)=(\S+)""".r
  val DefaultEth = "eth0"

  def argsToProps(args: Seq[String]): Map[String, String] =
    args.collect { case opt(key, value) ⇒ key -> value }(breakOut)

  def applySystemProperties(args: Map[String, String]) = {
    for ((key, value) ← args if key startsWith "-D") {
      println(s"SYSTEM VARIABLE: ${key substring 2} - $value")
      System.setProperty(key substring 2, value)
    }
  }

  if (args.size > 0) {
    val opts = argsToProps(args.toSeq)
    applySystemProperties(opts)
  }

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

  object SparkAnalytics extends MicroKernel(httpPort = httpP, ethName = eth) {
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
