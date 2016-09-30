package http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import com.github.scribejava.apis.TwitterApi
import com.github.scribejava.core.model.Verb
import http.oauth.OauthParams

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

//http://[host]:[port]/api/login-twitter
class TwitterLoginRouter(override val host: String, override val httpPort: Int,
                         override val httpPrefixAddress: String = "login-twitter")
                        (implicit val ec: ExecutionContext, val system: ActorSystem) extends SecuritySupport {
  private val referer = "Referer"

  implicit val params =  OauthParams(
    system.settings.config.getString("twitter.consumer-key"),
    system.settings.config.getString("twitter.consumer-secret"))

  private val twitterOauth = http.oauth.Oauth[com.github.scribejava.apis.TwitterApi]

  override implicit val timeout = akka.util.Timeout(5.seconds)

  private def twitterRoute(): Route =
    path(httpPrefixAddress) {
      get {
        val service = twitterOauth.oAuthService.callback(s"http://$host:$httpPort/$pathPrefix/twitter-sign-in").build(twitterOauth.instance)
        val requestToken = service.getRequestToken
        val url = service.getAuthorizationUrl(requestToken)
        redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
      }
    } ~ path("frontend-login-twitter") {
      get {
        headerValueByName(referer) { frontEndSegment ⇒
          system.log.info(s"frontend-login-from-twitter from:$frontEndSegment")
          val service = twitterOauth.oAuthService
            .callback(s"${frontEndSegment}twitter-callback")
            .build(twitterOauth.instance)
          val requestToken = service.getRequestToken
          val url = service.getAuthorizationUrl(requestToken)
          redirect(akka.http.scaladsl.model.Uri(url), StatusCodes.PermanentRedirect)
        }
      }
    } ~ path("twitter-sign-in") {
      get {
        parameters(('oauth_token.as[String]), ('oauth_verifier.as[String])) {
          (oauthToken, oauthVerifier) ⇒
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
                val service = twitterOauth.oAuthService.build(TwitterApi.instance())
                val requestToken = new com.github.scribejava.core.model.Token(oauthToken, oauthVerifier)
                val verifier = new com.github.scribejava.core.model.Verifier(oauthVerifier)
                val accessToken = service.getAccessToken(requestToken, verifier)
                val oAuthRequest = new com.github.scribejava.core.model.OAuthRequest(Verb.GET, twitterOauth.protectedUrl, service)
                service.signRequest(accessToken, oAuthRequest)
                val twitterResponse = oAuthRequest.send()
                if (twitterResponse.getCode == 200) {
                  val json = twitterResponse.getBody.parseJson.asJsObject
                  val user = json.getFields("name").head.toString().replace("\"", "")
                  s""" { "authorization-url": "http://$host:$httpPort/$pathPrefix/login?user=${user}_from_twitter&password=$oauthToken" }"""
                } else s"""{ "authorization-error": "${twitterResponse.getCode}" } """
              }(ec)
            }
        }
      }
    }

}
