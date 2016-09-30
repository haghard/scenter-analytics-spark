package spark

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import com.typesafe.config.ConfigFactory
import http._

object SparkQuerySupervisor {
  def props: Props =
    Props(new SparkQuerySupervisor).withDispatcher(SparkDispatcher)
}

class SparkQuerySupervisor extends Actor with ActorLogging {
  private val conf = ConfigFactory.load("application.conf")

  override val supervisorStrategy = OneForOneStrategy() {
    case reason: Throwable ⇒
      log.error("SparkJob error: {}", reason)
      Stop
  }

  override def receive: Receive = {
    case args: DefaultJobArgs ⇒
      context.actorOf(SparkJob.props(conf)) forward args
  }
}
