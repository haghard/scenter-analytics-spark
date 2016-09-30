package http

import java.util.concurrent.TimeUnit
import javax.ws.rs.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.github.scribejava.apis.{ GoogleApi, TwitterApi, GitHubApi }
import com.github.scribejava.core.model.{ OAuthRequest, Verb }
import com.softwaremill.session._
import akka.http.scaladsl.server._
import com.softwaremill.session.SessionDirectives._
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.CsrfOptions._
import com.softwaremill.session.SessionOptions._
import http.SparkJob.ResultsView
import io.swagger.annotations._

import scala.concurrent.{ Future, ExecutionContext }
import scala.util.Try
import org.mindrot.jbcrypt.BCrypt

trait SecuritySupport extends DefaultRestMicroservice /*with Directives*/ {
  def host: String
  def httpPort: Int

  implicit def ec: ExecutionContext
  implicit def system: ActorSystem

  //lazy val googleApiKey = system.settings.config.getString("google.consumer-key")
  //lazy val googleApiSecret = system.settings.config.getString("google.consumer-secret")
  //lazy val twitterApiKey = system.settings.config.getString("twitter.consumer-key")
  //lazy val twitterApiSecret = system.settings.config.getString("twitter.consumer-secret")
  lazy val githubApiKey = system.settings.config.getString("github.consumer-key")
  lazy val githubApiSecret = system.settings.config.getString("github.consumer-secret")

  val salt = BCrypt.gensalt()

  /*lazy val (google, twitter, github) = {
    (
      http.oauth.Oauth[com.github.scribejava.apis.GoogleApi20].withKeySecret(googleApiKey, googleApiSecret),
      http.oauth.Oauth[com.github.scribejava.apis.TwitterApi].withKeySecret(twitterApiKey, twitterApiSecret),
      http.oauth.Oauth[com.github.scribejava.apis.GitHubApi].withKeySecret(githubApiKey, githubApiSecret)
    )
  }*/

  implicit def serializer: SessionSerializer[ServerSession, String] =
    new SingleValueSessionSerializer({ session: ServerSession ⇒ (session.user + "-" + session.password) }, { v: (String) ⇒
      val kv = v.split("-")
      Try(ServerSession(kv(0), kv(1)))
    })

  val sessionConfig = SessionConfig.default(system.settings.config.getString("akka.http.session.server-secret"))
  implicit val sessionManager = new SessionManager[ServerSession](sessionConfig)

  implicit val refreshTokenStorage = new InMemoryRefreshTokenStorage[ServerSession] {
    def log(msg: String) = system.log.info(msg)
  }

  //oneOff vs refreshable; specifies what should happen when the session expires.
  //If refreshable and a refresh token is present, the session will be re-created
  def requiredHttpSession(implicit ec: ExecutionContext) =
    requiredSession(oneOff, usingHeaders)

  /*
  private def githubR(implicit ec: ExecutionContext): Route =
    path("login-github") {
      val service = github.oAuthService
        .callback(s"http://$host:$httpPort/$pathPrefix/github-sign-in").build(GitHubApi.instance)
      // Obtain the Authorization URL
      val url = service.getAuthorizationUrl()
      redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
    } ~ path("frontend-login-github") {
      get {
        extractHost { host ⇒
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
              val service = github.oAuthService.callback(s"http://$host:$httpPort/$pathPrefix/github-sign-in").build(GitHubApi.instance)
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
                s""" "authorizationUrl" : "http://$host:$httpPort/$pathPrefix/login?user=$user:github&password=$token" """
              } else s"""{ "authorization-error": "${response.getCode}" }"""
            }(ec)
          }
        }
      }
    }

  private def googleR(implicit ec: ExecutionContext): Route =
    path("login-google") {
      val service = google.oAuthService.callback(s"http://$host:$httpPort/$pathPrefix/google-sign-in").build(GoogleApi.instance)
      // Obtain the Authorization URL

      val requestToken = service.getRequestToken
      val url = service.getAuthorizationUrl(requestToken)
      redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
    } ~ path("google-sign-in") {
      get {
        parameterMap { params ⇒
          complete {
            Future {
              val service = google.oAuthService.callback(s"http://$host:$httpPort/$pathPrefix/google-sign-in").build(GoogleApi.instance)
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
                val user = json.fields("displayName").toString.replace("\"", "")
                s"$user has been authorized by google\nAuthorizationUrl: http://$host:$httpPort/$pathPrefix/login?user=$user:google&password=$token"
              } else response.getBody
            }(ec)
          }
        }
      }
    }
*/

  /*
  def login: Route = {
    pathPrefix(pathPrefix) {
      path("login") {
        get {
          parameters(('user.as[String]), ('password.as[String])) { (user, password) ⇒
            withUri { url ⇒
              setSession(oneOff, usingHeaders, ServerSession(user, BCrypt.hashpw(password, salt))) {
                setNewCsrfToken(checkHeader) { ctx ⇒
                  val age = system.settings.config.getDuration("http.session.max-age", TimeUnit.SECONDS)
                  ctx.complete(s"Welcome $user. Session age:$age sec")
                }
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
        } ~ twitterR ~ githubR ~ googleR  ~ path("test-secret") {
          get {
            requiredHttpSession(ec) { session ⇒
              complete(s"$session")
            }
          }
        }
    }
  }
  */
}
