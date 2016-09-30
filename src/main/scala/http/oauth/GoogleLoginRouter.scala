package http.oauth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.util.Timeout
import com.github.scribejava.apis.GoogleApi
import com.github.scribejava.core.model.{OAuthRequest, Verb}
import http.SecuritySupport

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class GoogleLoginRouter(override val host: String, override val httpPort: Int,
                        override val httpPrefixAddress: String = "login-google", pref: String)
                       (implicit val ec: ExecutionContext, val system: ActorSystem,
                        implicit val timeout: Timeout = Timeout(5 seconds)) extends SecuritySupport {

  implicit val params = OauthParams(
    system.settings.config.getString("google.consumer-key"),
    system.settings.config.getString("google.consumer-secret"))

  private val google = http.oauth.Oauth[com.github.scribejava.apis.GoogleApi20]

  val route = googleRoute

  private def googleRoute(): Route =
    pathPrefix(pref) {
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
    }
}
