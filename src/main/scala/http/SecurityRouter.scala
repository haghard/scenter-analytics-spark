package http

import akka.http.scaladsl.model.StatusCodes
import com.github.scribejava.core.model.Verb
import com.softwaremill.session._
import akka.http.scaladsl.server._
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.SessionDirectives._

import scala.concurrent.{ Future, ExecutionContext }
import scala.util.Try
import org.mindrot.jbcrypt.BCrypt

trait SecurityRouter extends DefaultRestMicroservice with Directives
    with TwitterOauth { mixin: MicroKernel ⇒

  val salt = BCrypt.gensalt()

  val PROTECTED_RESOURCE_URL = "https://api.twitter.com/1.1/account/verify_credentials.json"

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
        urls = s"[$httpPrefixAddress/$pathPrefix/login, $httpPrefixAddress/$pathPrefix/login-twitter, $httpPrefixAddress/$pathPrefix/logout]")

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
                complete(s"Invalidated: $session")
              }
            }
          }
        } ~
        path("login-twitter") {
          get {
            val service = oAuthService()
              .callback(s"http://$localAddress:$httpPort/$pathPrefix/twitter-sign-in")
              .build()
            val requestToken = service.getRequestToken
            val url = service.getAuthorizationUrl(requestToken)
            redirect(
              akka.http.scaladsl.model.Uri(url),
              StatusCodes.PermanentRedirect)
          }
        } ~
        path("twitter-sign-in") {
          get {
            parameters(('oauth_token.as[String]), ('oauth_verifier.as[String])) { (oauthToken, oauthVerifier) ⇒
              /**
               * Converting the request token to an access token
               *
               * To render the request token into a usable access token,
               * your application must make a request to the POST oauth / access_token endpoint,
               * containing the oauth_verifier value obtained in prev step.
               * The request token is also passed in the oauth_token portion of the header,
               * but this will have been added by the signing process.
               *
               * Source https://dev.twitter.com/web/sign-in/implementing
               */
              complete {
                Future {
                  val service = oAuthService().build()
                  val requestToken = new com.github.scribejava.core.model.Token(oauthToken, oauthVerifier)
                  val verifier = new com.github.scribejava.core.model.Verifier(oauthVerifier)
                  val accessToken = service.getAccessToken(requestToken, verifier)
                  val oAuthRequest = new com.github.scribejava.core.model.OAuthRequest(Verb.GET, PROTECTED_RESOURCE_URL, service)
                  service.signRequest(accessToken, oAuthRequest)
                  val twitterResponse = oAuthRequest.send()
                  import spray.json._
                  val json = twitterResponse.getBody.parseJson.asJsObject
                  val user = json.getFields("name").head.toString().replace("\"", "")
                  s"$user was authorized through twitter\nLogin app link: http://$localAddress:$httpPort/$pathPrefix/login?user=$user&password=$oauthToken"
                }
              }
            }
          }
        } ~ path("test-secret") {
          get {
            requiredHttpSession(ec) { session ⇒
              complete(s"$session")
            }
          }
        }
    }
  }
}