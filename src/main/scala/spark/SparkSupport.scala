package spark

import akka.actor.ActorRef
import org.apache.spark.SparkContext

trait SparkSupport {
  def guardian: ActorRef
  def context: SparkContext
}
