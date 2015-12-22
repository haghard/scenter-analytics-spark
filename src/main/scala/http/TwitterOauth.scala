package http

import com.github.scribejava.core.builder.ServiceBuilder
import com.github.scribejava.apis.TwitterApi

trait TwitterOauth {

  def consumerKey: String
  def consumerSecret: String

  def oAuthService() = {
    new ServiceBuilder().provider(classOf[TwitterApi])
      .apiKey(consumerKey)
      .apiSecret(consumerSecret)
  }
}

