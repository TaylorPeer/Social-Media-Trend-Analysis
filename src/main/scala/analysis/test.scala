package analysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test {

  var sparkContext: SparkContext = null

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SEC")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.executor.memory", "12g")
    sparkContext = new SparkContext(sparkConf)
  }

}