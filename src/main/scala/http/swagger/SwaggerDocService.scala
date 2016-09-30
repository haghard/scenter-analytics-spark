package http.swagger

import http._

import scala.reflect.runtime.{ universe => ru }
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.github.swagger.akka._
import com.github.swagger.akka.model.Info
import io.swagger.models.ExternalDocs
import io.swagger.models.auth.{ ApiKeyAuthDefinition, BasicAuthDefinition }

class SwaggerDocService(system: ActorSystem, hostLine: String) extends SwaggerHttpService with HasActorSystem {
  override implicit val actorSystem: ActorSystem = system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override val apiTypes = Seq(ru.typeOf[LoginRouter], ru.typeOf[ResultsRouter], ru.typeOf[DailyResultsRouter],
    ru.typeOf[PlayerStatRouter], ru.typeOf[PtsLeadersRouter])
  override val host = hostLine
  override val info = Info(version = "1.0")
  override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
  override val securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition) //new ApiKeyAuthDefinition()
}