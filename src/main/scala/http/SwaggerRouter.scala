package http

import akka.actor.ActorSystem
import akka.util.Timeout
import scala.concurrent.duration._
import http.swagger.SwaggerDocService
import scala.concurrent.ExecutionContext

class SwaggerRouter(override val host: String, override val httpPort: Int)
                    (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport {
  //mixin: MicroKernel ⇒

  val route =
    corsHandler(new SwaggerDocService(system, s"${host}:${httpPort}").routes)

  /*abstract override def configureApi() =
    super.configureApi() ~ Api(
      route = Option { ec: ExecutionContext ⇒ corsHandler(new SwaggerDocService(system, s"${localAddress}:${httpPort}").routes) },
      postAction = Option(() ⇒ system.log.info(s"★ ★ ★ Enable SwaggerDoc ★ ★ ★")),
      urls = "")*/


  override implicit val timeout = Timeout(1.seconds)
  override protected val httpPrefixAddress: String = ""
}
