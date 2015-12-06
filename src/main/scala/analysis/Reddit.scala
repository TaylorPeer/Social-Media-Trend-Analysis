package analysis

import java.io.File

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * TODO
 */
object Reddit {
  
  val DataLocation = "/Users/taylorpeer/Projects/BI/data/"

  var sc: SparkContext = null

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RA")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.executor.memory", "12g")
    sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    // More info: http://spark.apache.org/docs/latest/sql-programming-guide.html#json-datasets
    val subreddits = sqlContext.read.json(DataLocation + "subreddits")
    subreddits.printSchema()
    subreddits.registerTempTable("subreddits")
    
    val subredditsByLanguages = sqlContext.sql("SELECT lang, count(*) FROM subreddits GROUP BY lang")
  }

}