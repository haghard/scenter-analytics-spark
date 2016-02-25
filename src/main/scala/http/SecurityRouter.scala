package http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.directives.BasicDirectives._
import com.github.scribejava.apis.{GoogleApi, TwitterApi, GitHubApi}
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

  val senderAddress = "X-Forwarded-For"

  def googleApiKey: String
  def googleApiSecret: String
  def twitterApiKey: String
  def twitterApiSecret: String
  def githubApiKey: String
  def githubApiSecret: String

  val salt = BCrypt.gensalt()

  lazy val (google, twitter, github) = {
    (http.oauth.Oauth[com.github.scribejava.apis.GoogleApi20].withKeySecret(googleApiKey, googleApiSecret),
      http.oauth.Oauth[com.github.scribejava.apis.TwitterApi].withKeySecret(twitterApiKey, twitterApiSecret),
      http.oauth.Oauth[com.github.scribejava.apis.GitHubApi].withKeySecret(githubApiKey, githubApiSecret))
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

  //oneOff vs refreshable; specifies what should happen when the session expires.
  //If refreshable and a refresh token is present, the session will be re-created
  def requiredHttpSession(implicit ec: ExecutionContext) = requiredSession(oneOff, usingHeaders)

  //https://github.com/softwaremill/akka-http-session
  abstract override def configureApi() =
    super.configureApi() ~
      Api(route = Option { ec: ExecutionContext ⇒ securityRoute(ec) },
        postAction = Option(() ⇒ system.log.info(s"\n★ ★ ★ [$httpPrefixAddress/login|logout] routes was stopped on $httpPrefixAddress ★ ★ ★")),
        urls = s"[$httpPrefixAddress/$pathPrefix/login, $httpPrefixAddress/$pathPrefix/login-twitter, $httpPrefixAddress/$pathPrefix/login-github, $httpPrefixAddress/$pathPrefix/login-google, $httpPrefixAddress/$pathPrefix/logout]")

  private def githubR(implicit ec: ExecutionContext): Route =
    path("login-github") {
      val service = github.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/github-sign-in").build(GitHubApi.instance)
      // Obtain the Authorization URL
      val url = service.getAuthorizationUrl()
      redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
    } ~ path("frontend-login-github") {
      get {
        extractHost { host =>
          system.log.info(s"frontend-login-github from: $host")
          //FIXME port 9000 put address in the params
          val service = github.oAuthService.callback(s"http://$host:9000/github-callback").build(GitHubApi.instance)
          //val requestToken = service.getRequestToken
          val url = service.getAuthorizationUrl()
          system.log.info(s"frontend-login-github url: $url")
          redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
        }
      }
    } ~ path("github-sign-in") {
      get {
        parameterMap { params ⇒
          complete {
            Future {
              val service = github.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/github-sign-in").build(GitHubApi.instance)
              val (k, v) = params.head
              val verifier = new com.github.scribejava.core.model.Verifier(v)

              // Obtain the AccessToken
              val accessToken = service.getAccessToken(verifier)
              val token = accessToken.getToken

              val request = new OAuthRequest(Verb.GET, github.protectedUrl, service)
              service.signRequest(accessToken, request)

              val response = request.send
              if (response.getCode == 200) {
                import spray.json._
                val json = response.getBody.parseJson.asJsObject
                val user = json.fields("name").toString().replace("\"", "")
                s""" "authorizationUrl" : "http://$domain:$httpPort/$pathPrefix/login?user=$user:github&password=$token" """
              } else s"""{ "authorization-error": "${response.getCode}" }"""
            }(ec)
          }
        }
      }
    }

  private def googleR(implicit ec: ExecutionContext): Route =
    path("login-google") {
      val service = google.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/google-sign-in").build(GoogleApi.instance)
      // Obtain the Authorization URL

      val requestToken = service.getRequestToken
      val url = service.getAuthorizationUrl(requestToken)
      redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
    } ~ path("google-sign-in") {
      get {
        parameterMap { params ⇒
          complete {
            Future {
              val service = google.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/google-sign-in").build(GoogleApi.instance)
              val (k, v) = params.head
              val verifier = new com.github.scribejava.core.model.Verifier(v)

              // Obtain the AccessToken
              val accessToken = service.getAccessToken(null, verifier)
              val token = accessToken.getToken

              val request = new OAuthRequest(Verb.GET, google.protectedUrl, service)
              service.signRequest(accessToken, request)

              val response = request.send

              import spray.json._
              if (response.getCode == 200) {
                val googleResponse = response.getBody
                val json = googleResponse.parseJson.asJsObject
                val user = json.fields("displayName").toString().replace("\"", "")
                s"$user has been authorized by google\nAuthorizationUrl: http://$domain:$httpPort/$pathPrefix/login?user=$user:google&password=$token"
              } else response.getBody
            }(ec)
          }
        }
      }
    }

  private def twitterR(implicit ec: ExecutionContext): Route =
    path("login-twitter") {
      get {
        extractHost { host =>
          system.log.info(s"login-from-twitter from: $host")
          val service = twitter.oAuthService.callback(s"http://$domain:$httpPort/$pathPrefix/twitter-sign-in").build(twitter.instance)
          val requestToken = service.getRequestToken
          val url = service.getAuthorizationUrl(requestToken)
          system.log.info(s"login-twitter: $host $url")
          redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
        }
      }
    } ~ path("frontend-login-twitter") {
      get {
        extract(_.request.headers) { headers =>
          system.log.info(headers.mkString(", "))
        //headerValueByName("Location") { senderAddress =>
          system.log.info(s"frontend-login-from-twitter from:")
          val service = twitter.oAuthService.callback(s"http://twitter-callback").build(twitter.instance)
          val requestToken = service.getRequestToken
          val url = service.getAuthorizationUrl(requestToken)
          redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
        }
      }
    } ~ path("twitter-sign-in") {
      get {
        parameters(('oauth_token.as[String]), ('oauth_verifier.as[String])) { (oauthToken, oauthVerifier) ⇒
          /**
           * Converting the request token to an access token
           * To render the request token into a usable access token,
           * your application must make a request to the POST oauth / access_token endpoint,
           * containing the oauth_verifier value obtained in prev step.
           * The request token is also passed in the oauth_token portion of the header,
           * but this will have been added by the signing process.
           *
           * Source https://dev.twitter.com/web/sign-in/implementing
           */
          import spray.json._
          complete {
            Future {
              val service = twitter.oAuthService.build(TwitterApi.instance())
              val requestToken = new com.github.scribejava.core.model.Token(oauthToken, oauthVerifier)
              val verifier = new com.github.scribejava.core.model.Verifier(oauthVerifier)
              val accessToken = service.getAccessToken(requestToken, verifier)
              val oAuthRequest = new com.github.scribejava.core.model.OAuthRequest(Verb.GET, twitter.protectedUrl, service)
              service.signRequest(accessToken, oAuthRequest)
              val twitterResponse = oAuthRequest.send()
              if (twitterResponse.getCode == 200) {
                val json = twitterResponse.getBody.parseJson.asJsObject
                val user = json.getFields("name").head.toString().replace("\"", "")
                s""" { "authorization-url": "http://$domain:$httpPort/$pathPrefix/login?user=$user:twitter&password=$oauthToken" }"""
              } else s"""{ "authorization-error": "${twitterResponse.getCode}" } """
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
              setSession(oneOff, usingHeaders, ServerSession(user, BCrypt.hashpw(password, salt))) {
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
                invalidateSession(oneOff, usingHeaders)
                complete(s"Invalidated: $session")
              }
            }
          }
        } ~ twitterR ~ githubR ~ googleR ~
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