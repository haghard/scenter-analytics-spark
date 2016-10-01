package spark

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import com.typesafe.config.ConfigFactory
import http._

object SparkProgramGuardian {
  def props: Props = Props(new SparkProgramGuardian).withDispatcher(SparkDispatcher)
}

class SparkProgramGuardian extends Actor with ActorLogging {
  private val conf = ConfigFactory.load("application.conf")

  override val supervisorStrategy = OneForOneStrategy() {
    case reason: Throwable ⇒
      log.error(reason, "SparkProgram has failed")
      Stop
  }

  override def receive: Receive = {
    case args: DefaultJobArgs ⇒
      context.actorOf(SparkProgram.props(conf)) forward args
  }
}
