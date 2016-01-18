package http

import akka.http.scaladsl.model.StatusCodes
import com.github.scribejava.core.model.{ OAuthRequest, Verb }
import com.softwaremill.session._
import akka.http.scaladsl.server._
import com.softwaremill.session.SessionDirectives._
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.CsrfOptions._
import com.softwaremill.session.SessionOptions._

import scala.concurrent.{ Future, ExecutionContext }
import scala.util.Try
import org.mindrot.jbcrypt.BCrypt

trait SecurityRouter extends DefaultRestMicroservice with Directives { mixin: MicroKernel ⇒

  def googleApiKey: String
  def googleApiSecret: String
  def twitterApiKey: String
  def twitterApiSecret: String
  def githubApiKey: String
  def githubApiSecret: String

  val salt = BCrypt.gensalt()

  lazy val googleOauth = {
    val local = http.oauth.Oauth[com.github.scribejava.apis.GoogleApi20]
    local.setKeySecret(googleApiKey, googleApiSecret)
  }

  lazy val twitterOauth = {
    val local = http.oauth.Oauth[com.github.scribejava.apis.TwitterApi]
    local.setKeySecret(twitterApiKey, twitterApiSecret)
  }

  lazy val githubOauth = {
    val local = http.oauth.Oauth[com.github.scribejava.apis.GitHubApi]
    local.setKeySecret(githubApiKey, githubApiSecret)
  }

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

  //refreshable

  def requiredHttpSession(implicit ec: ExecutionContext) = requiredSession(oneOff, usingCookies)

  //https://github.com/softwaremill/akka-http-session
  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ securityRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ [$httpPrefixAddress/login|logout] routes was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/$pathPrefix/login, $httpPrefixAddress/$pathPrefix/login-twitter, $httpPrefixAddress/$pathPrefix/logout]")

  private def github(implicit ec: ExecutionContext): Route =
    path("login-github") {
      val service = githubOauth.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/github-sign-in").build()
      // Obtain the Authorization URL
      val url = service.getAuthorizationUrl(null)
      redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
    } ~ path("github-sign-in") {
      get {
        parameterMap { params ⇒
          complete {
            Future {
              val service = githubOauth.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/github-sign-in").build()
              val (k, v) = params.head
              val verifier = new com.github.scribejava.core.model.Verifier(v)

              // Obtain the AccessToken
              val accessToken = service.getAccessToken(null, verifier)
              val token = accessToken.getToken

              val request = new OAuthRequest(Verb.GET, githubOauth.protectedUrl, service)
              service.signRequest(accessToken, request)

              val response = request.send

              import spray.json._
              val json = response.getBody.parseJson.asJsObject
              val user = json.fields("name").toString().replace("\"", "")
              s"$user has been authorized by github\nAuthorizationUrl: http://$localAddress:$httpPort/$pathPrefix/login?user=$user:github&password=$token"
            }(ec)
          }
        }
      }
    }

  private def google(implicit ec: ExecutionContext): Route =
    path("login-google") {
      val service = googleOauth.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/google-sign-in").build()
      // Obtain the Authorization URL
      val url = service.getAuthorizationUrl(null)
      redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
    } ~ path("google-sign-in") {
      get {
        parameterMap { params ⇒
          complete {
            Future {
              val service = googleOauth.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/google-sign-in").build()
              val (k, v) = params.head
              val verifier = new com.github.scribejava.core.model.Verifier(v)

              // Obtain the AccessToken
              val accessToken = service.getAccessToken(null, verifier)
              val token = accessToken.getToken

              val request = new OAuthRequest(Verb.GET, googleOauth.protectedUrl, service)
              service.signRequest(accessToken, request)

              val response = request.send

              import spray.json._
              val googleResponse = response.getBody

              val json = googleResponse.parseJson.asJsObject
              val user = json.fields("displayName").toString().replace("\"", "")

              s"$user has been authorized by google\nAuthorizationUrl: http://$localAddress:$httpPort/$pathPrefix/login?user=$user:google&password=$token"
            }(ec)
          }
        }
      }
    }

  private def twitter(implicit ec: ExecutionContext): Route =
    path("login-twitter") {
      get {
        val service = twitterOauth.oAuthService.callback(s"http://$localAddress:$httpPort/$pathPrefix/twitter-sign-in").build()
        val requestToken = service.getRequestToken
        val url = service.getAuthorizationUrl(requestToken)
        redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
      }
    } ~ path("twitter-sign-in") {
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
              val service = twitterOauth.oAuthService.build()
              val requestToken = new com.github.scribejava.core.model.Token(oauthToken, oauthVerifier)
              val verifier = new com.github.scribejava.core.model.Verifier(oauthVerifier)
              val accessToken = service.getAccessToken(requestToken, verifier)
              val oAuthRequest = new com.github.scribejava.core.model.OAuthRequest(Verb.GET, twitterOauth.protectedUrl, service)
              service.signRequest(accessToken, oAuthRequest)
              val twitterResponse = oAuthRequest.send()
              import spray.json._
              val json = twitterResponse.getBody.parseJson.asJsObject
              val user = json.getFields("name").head.toString().replace("\"", "")
              s"$user has been authorized by twitter\nAuthorizationUrl: http://$localAddress:$httpPort/$pathPrefix/login?user=$user:twitter&password=$oauthToken"
            }(ec)
          }
        }
      }
    }

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
        } ~ twitter ~ google ~ github ~
        path("test-secret") {
          get {
            requiredHttpSession(ec) { session ⇒
              complete(s"$session")
            }
          }
        }
    }
  }
}