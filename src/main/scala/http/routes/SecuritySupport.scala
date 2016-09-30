package http.routes

import akka.actor.ActorSystem
import com.softwaremill.session.SessionDirectives._
import com.softwaremill.session.SessionOptions._
import com.softwaremill.session._
import http.{DefaultRestMicroservice, ServerSession}
import org.mindrot.jbcrypt.BCrypt

import scala.concurrent.ExecutionContext
import scala.util.Try

trait SecuritySupport extends DefaultRestMicroservice {
  def host: String
  def httpPort: Int

  implicit def ec: ExecutionContext
  implicit def system: ActorSystem

  //val googleApiKey = system.settings.config.getString("google.consumer-key")
  //val googleApiSecret = system.settings.config.getString("google.consumer-secret")

  val salt = BCrypt.gensalt()

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

}
