package http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import scala.concurrent.duration._
import http.swagger.SwaggerDocService
import scala.concurrent.ExecutionContext

//https://blog.codecentric.de/en/2016/04/swagger-akka-http/
class SwaggerDocRouter(override val host: String, override val httpPort: Int)
                      (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport {
  //mixin: MicroKernel ⇒

  //http://localhost:8001/api-docs/swagger.json
  val assets =
    pathPrefix("swagger") {
      getFromResourceDirectory("swagger") ~ pathSingleSlash(get(redirect("index.html", StatusCodes.PermanentRedirect)))
    }

  val route = corsHandler(new SwaggerDocService(system, s"${host}:${httpPort}").routes) ~ assets

  /*abstract override def configureApi() =
    super.configureApi() ~ Api(
      route = Option { ec: ExecutionContext ⇒ corsHandler(new SwaggerDocService(system, s"${localAddress}:${httpPort}").routes) },
      postAction = Option(() ⇒ system.log.info(s"★ ★ ★ Enable SwaggerDoc ★ ★ ★")),
      urls = "")*/


  override implicit val timeout = Timeout(1.seconds)
  override protected val httpPrefixAddress: String = ""
}
