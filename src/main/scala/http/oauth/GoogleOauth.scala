package http.oauth

import com.github.scribejava.apis.GoogleApi20
import com.github.scribejava.core.builder.ServiceBuilder

trait GoogleOauth {

  def googleApiKey: String
  def googleApiSecret: String

  val googleProtectedUrl = "https://www.googleapis.com/plus/v1/people/me"

  def googleOauthService(): ServiceBuilder =
    new ServiceBuilder().provider(classOf[GoogleApi20])
      .apiKey(googleApiKey)
      .apiSecret(googleApiSecret)
      .scope("profile")
}