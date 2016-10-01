package spark

import org.apache.spark.SparkContext

trait SparkSupport {
  def sparkContext: SparkContext
}
