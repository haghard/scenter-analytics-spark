package http

import scala.util.Try
import scala.collection._

object Bootstrap extends App {
  val opt = """--(\S+)=(\S+)""".r
  val DefaultEth = "eth0"

  def argsToProps(args: Array[String]) = args.collect { case opt(key, value) ⇒ key -> value }(breakOut)

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

  object SparkAnalytics extends MicroKernel(httpPort = httpP, ethName = eth) with PlayersRouter {
    import scala.concurrent.duration._
    implicit val timeout = akka.util.Timeout(60 seconds)
    override val environment = "Spark"

    override lazy val consumerKey = system.settings.config.getString("twitter.consumer-key")
    override lazy val consumerSecret = system.settings.config.getString("twitter.consumer-secret")
  }

  SparkAnalytics.startup

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run = SparkAnalytics.shutdown
  }))
}