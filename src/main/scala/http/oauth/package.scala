package http

import com.github.scribejava.apis.{ GitHubApi, TwitterApi, GoogleApi20 }

import scala.annotation.implicitNotFound
import java.util.concurrent.ThreadLocalRandom

package object oauth {

  case class OauthParams(apiKey: String, apiSecret: String)

  trait Oauth[T <: com.github.scribejava.core.builder.api.Api] {
    def protectedUrl: String

    protected def secretState =
      s"secret${ThreadLocalRandom.current().nextInt(Int.MinValue, Int.MaxValue)}"

    def oAuthService: com.github.scribejava.core.builder.ServiceBuilder

    def instance: T
  }

  object Oauth {
    @implicitNotFound(msg = "Cannot find Oauth type class for ${T}")
    def apply[T <: com.github.scribejava.core.builder.api.Api: Oauth]: Oauth[T] = implicitly[Oauth[T]]

    implicit def google(implicit c: OauthParams) = new Oauth[com.github.scribejava.apis.GoogleApi20] {
      override val protectedUrl =
        "https://www.googleapis.com/plus/v1/people/me"

      override def oAuthService() =
        new com.github.scribejava.core.builder.ServiceBuilder() //.provider(classOf[com.github.scribejava.apis.GoogleApi20])
          .apiKey(c.apiKey)
          .apiSecret(c.apiSecret)
          .state(secretState)
          .scope("profile")

      override def instance: GoogleApi20 = ???
    }

    implicit def twitter(implicit c: OauthParams) = new Oauth[com.github.scribejava.apis.TwitterApi] {
      override val protectedUrl =
        "https://api.twitter.com/1.1/account/verify_credentials.json"

      override def oAuthService() =
        new com.github.scribejava.core.builder.ServiceBuilder()
          .apiKey(c.apiKey)
          .apiSecret(c.apiSecret)

      override def instance: TwitterApi = TwitterApi.instance()
    }

    implicit def github(implicit c: OauthParams) = new Oauth[com.github.scribejava.apis.GitHubApi] {
      override val protectedUrl = "https://api.github.com/user"

      override def oAuthService() =
        new com.github.scribejava.core.builder.ServiceBuilder()
          .apiKey(c.apiKey)
          .apiSecret(c.apiSecret)
          .state(secretState)

      override def instance: GitHubApi = GitHubApi.instance()
    }
  }
}
