package http

import akka.actor._
import com.typesafe.config.ConfigFactory
import akka.actor.SupervisorStrategy.Stop

object SparkQuerySupervisor {
  def props: Props =
    Props(new SparkQuerySupervisor).withDispatcher(SparkDispatcher)
}

class SparkQuerySupervisor extends Actor with ActorLogging {
  private val conf = ConfigFactory.load("application.conf")

  override val supervisorStrategy = OneForOneStrategy() {
    case reason: Exception ⇒
      log.debug(
        "SparkJobSupervisor has caught unexpected error: {}",
        reason.getMessage
      )
      Stop
  }

  override def receive: Receive = {
    case args: DefaultJobArgs ⇒
      context.actorOf(SparkJob.props(conf)) forward args
  }
}
