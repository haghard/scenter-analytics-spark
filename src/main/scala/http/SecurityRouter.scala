package http

import com.softwaremill.session._
import akka.http.scaladsl.server._
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.SessionDirectives._

import scala.concurrent.ExecutionContext
import scala.util.Try
import org.mindrot.jbcrypt.BCrypt

trait SecurityRouter extends DefaultRestMicroservice with Directives { mixin: MicroKernel ⇒
  val salt = BCrypt.gensalt()

  implicit def serializer: SessionSerializer[ServerSession, String] =
    new SingleValueSessionSerializer({ session: ServerSession ⇒ (session.user + "-" + session.password) }, { v: (String) ⇒
      val kv = v.split("-")
      Try(ServerSession(kv(0), kv(1)))
    })

  val sessionConfig = SessionConfig.default(system.settings.config.getString("http.secret"))
  implicit val sessionManager = new SessionManager[ServerSession](sessionConfig)

  implicit val refreshTokenStorage = new InMemoryRefreshTokenStorage[ServerSession] {
    def log(msg: String) = system.log.info(msg)
  }

  def requiredHttpSession(implicit ec: ExecutionContext) = requiredSession(refreshable, usingCookies)

  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ securityRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ [$httpPrefixAddress/login|logout] routes was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/login, $httpPrefixAddress/logout]")

  def securityRoute(implicit ec: ExecutionContext): Route = {
    pathPrefix(pathPrefix) {
      path("login") {
        get {
          parameters(('user.as[String]), ('password.as[String])) { (user, password) ⇒
            withUri { url ⇒
              setSession(refreshable, usingCookies, ServerSession(user, BCrypt.hashpw(password, salt))) {
                setNewCsrfToken(checkHeader) { ctx ⇒ ctx.complete(s"$user was logged in") }
              }
            }
          }
        }
      } ~
        path("logout") {
          get {
            withUri { url ⇒
              requiredHttpSession(ec) { session ⇒
                invalidateSession(refreshable, usingCookies)
                get(complete(s"Invalidated: $session"))
              }
            }
          }
        }
    }
  }
}