package http

import com.github.scribejava.apis.{GitHubApi, TwitterApi, GoogleApi20}

import scala.annotation.implicitNotFound
import java.util.concurrent.ThreadLocalRandom

package object oauth {

  trait Oauth[T <: com.github.scribejava.core.builder.api.Api] {
    var apiKey: String = _
    var apiSecret: String = _

    def protectedUrl: String

    protected def secretState = s"secret${ThreadLocalRandom.current().nextInt(Int.MinValue, Int.MaxValue)}"

    def oAuthService: com.github.scribejava.core.builder.ServiceBuilder

    def instance: T

    def withKeySecret(apiKey0: String, apiSecret0: String): Oauth[T] = {
      apiKey = apiKey0
      apiSecret = apiSecret0
      this
    }
  }

  object Oauth {

    @implicitNotFound(msg = "Cannot find Oauth type class for ${T}")
    def apply[T <: com.github.scribejava.core.builder.api.Api: Oauth]: Oauth[T] = implicitly[Oauth[T]]

    implicit def google = new Oauth[com.github.scribejava.apis.GoogleApi20] {
      override val protectedUrl = "https://www.googleapis.com/plus/v1/people/me"

      override def oAuthService() =
        new com.github.scribejava.core.builder.ServiceBuilder()//.provider(classOf[com.github.scribejava.apis.GoogleApi20])
          .apiKey(apiKey)
          .apiSecret(apiSecret)
          .state(secretState)
          .scope("profile")

      override def instance: GoogleApi20 = ???
    }

    implicit def twitter = new Oauth[com.github.scribejava.apis.TwitterApi] {
      override val protectedUrl = "https://api.twitter.com/1.1/account/verify_credentials.json"

      override def oAuthService() =
        new com.github.scribejava.core.builder.ServiceBuilder()
          .apiKey(apiKey)
          .apiSecret(apiSecret)

      override def instance: TwitterApi = TwitterApi.instance()
    }

    implicit def github = new Oauth[com.github.scribejava.apis.GitHubApi] {
      override val protectedUrl = "https://api.github.com/user"

      override def oAuthService() =
        new com.github.scribejava.core.builder.ServiceBuilder()
          .apiKey(apiKey)
          .apiSecret(apiSecret)
          .state(secretState)

      override def instance: GitHubApi = GitHubApi.instance()
    }
  }
}