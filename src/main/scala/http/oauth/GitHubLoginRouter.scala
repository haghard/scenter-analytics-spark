package http.oauth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.github.scribejava.apis.{ GitHubApi, GoogleApi }
import com.github.scribejava.core.model.{ OAuthRequest, Verb }
import http.routes.SecuritySupport

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

//http://[host]:[port]/api/login-github
class GitHubLoginRouter(override val host: String, override val httpPort: Int,
    override val httpPrefixAddress: String = "login-github", pref: String)(implicit val ec: ExecutionContext, val system: ActorSystem,
    implicit val timeout: Timeout = Timeout(5 seconds)) extends SecuritySupport {

  implicit val params = OauthParams(
    system.settings.config.getString("github.consumer-key"),
    system.settings.config.getString("github.consumer-secret")
  )

  private val github = http.oauth.Oauth[com.github.scribejava.apis.GitHubApi]

  val route = allRoute()

  private def allRoute(): Route =
    pathPrefix(pref) {
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
            val service = github.oAuthService.callback(s"http://$host:$httpPort/github-callback").build(GitHubApi.instance)
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
                } else
                  s"""{ "authorization-error": "${response.getCode}" }"""
              }(ec)
            }
          }
        }
      }
    }
}