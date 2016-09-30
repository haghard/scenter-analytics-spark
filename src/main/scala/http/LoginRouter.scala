package http

import java.util.concurrent.TimeUnit
import javax.ws.rs.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import akka.util.ByteString
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.CsrfOptions._
import com.softwaremill.session.SessionDirectives._
import com.softwaremill.session.SessionOptions._
import io.swagger.annotations._
import org.mindrot.jbcrypt.BCrypt
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

@io.swagger.annotations.Api(value = "/login", produces = "application/json")
@Path("/api/login")
class LoginRouter(override val host: String, override val httpPort: Int, override val httpPrefixAddress: String = "login")
                 (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport {
  override implicit val timeout = akka.util.Timeout(3.seconds)
  private val age = system.settings.config.getDuration("akka.http.session.max-age", TimeUnit.SECONDS)

  val route = login

  @ApiOperation(value = "Login with username and password", notes = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "user", value = "Username", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "password", value = "Password", required = true, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[String]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  def login: Route = {
    pathPrefix(pathPrefix) {
      path("login") {
        get {
          parameters(('user.as[String]), ('password.as[String])) { (user, password) ⇒
            withUri { url ⇒
              setSession(oneOff, usingHeaders, ServerSession(user, BCrypt.hashpw(password, salt))) {
                setNewCsrfToken(checkHeader) { ctx ⇒
                  //system.log.info(s"$user $password")
                  ctx.complete {
                    HttpResponse(StatusCodes.OK, scala.collection.immutable.Seq[HttpHeader](),
                      HttpEntity(ContentTypes.`application/json`, ByteString(s"{user:$user, session-age:$age}".toJson.prettyPrint))
                    )
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
