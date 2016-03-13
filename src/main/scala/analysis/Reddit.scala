package analysis

import java.io.File
import java.io.File
import scala.util.matching.Regex
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

object Reddit {

  var sc: SparkContext = null

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RA")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.executor.memory", "12g")
    sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    case class Post(
      text: String,
      created_at: String,
      platform: String,
      popularity: Long)

    /**
     * Load Twitter Data
     */
    def loadTwitterData(Path: String): RDD[Post] = {
      val df = sqlContext.read.json(Path)
      val postsRDD = df.map {
        row =>
          val textIndex = row.fieldIndex("text")
          val createdAtIndex = row.fieldIndex("created_at")
          val reweetsIndex = row.fieldIndex("retweet_count")
          val text = row.getString(textIndex)
          val createdAt = row.getString(createdAtIndex)
          val popularity = row.getString(reweetsIndex).replaceAll("\\+", "").toLong
          Post(text, createdAt, "Twitter", popularity)
      }
      return postsRDD
    }

    /**
     * Load Reddit Data
     */
    def loadRedditData(Path: String): RDD[Post] = {
      val df = sqlContext.read.json(Path)
      val postsRDD = df.map {
        row =>
          val textIndex = row.fieldIndex("body")
          val createdAtIndex = row.fieldIndex("created_utc")
          
          import java.time._
          val utcZoneId = ZoneId.of("UTC")
          val zonedDateTime = ZonedDateTime.now
          val utcDateTime = zonedDateTime.withZoneSameInstant(utcZoneId)
          
          
          val ups = row.fieldIndex("ups")
          val text = row.getString(textIndex)
          val createdAt = row.getString(createdAtIndex)
          val popularity = row.getLong(ups)
          Post(text, createdAt, "Reddit", popularity)
      }
      return postsRDD
    }

    var tweetsRDD = loadTwitterData("/Users/taylorpeer/Projects/BI/data/twitter/merged.json") // merged.json
    var redditRDD = loadRedditData("/Users/taylorpeer/Projects/BI/data/comments/merged.json") // RC_2012-01.json

    // println(tweetsRDD.count() + " Tweets loaded.")
    // println(redditRDD.count() + " Reddit comments loaded.")

    // Load stopwords
    val Stopwords = sc.textFile("/Users/taylorpeer/Projects/BI/stopwords.txt").collect.toSet

    /**
     * Computes the top terms contained in a Post RDD
     */
    def computeTopTerms(postRdd: RDD[Post], count: Int): Array[(String, Int)] = {

      val words = postRdd.flatMap(t => t.text.toLowerCase().replaceAll("\\.", "").replaceAll("\\,", "").trim().split(" "))
      val filteredWords = words.filter(word => !Stopwords.contains(word)).filter(word => word.length() > 1)
      val wordCounts = filteredWords.map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2)
      val topWords = wordCounts.takeOrdered(count)(Ordering[Int].reverse.on(x => x._2))

      return topWords;
    }

    redditRDD = redditRDD.filter(post => post.popularity >= 100)
    val wordCounts = computeTopTerms(redditRDD, 20)
    println(redditRDD.count() + " redditRDD posts loaded.")
    wordCounts.foreach(println)
    
    var output = ""
    wordCounts.foreach(l => output = output + l._1 + "\t" + l._2 + "\n")

    def convertOccurencesToRanks(termOccurences: Array[(String, Int)]): Array[(String, Int)] = {
      val sorted = termOccurences.sortBy(_._2).reverse
      var rank = 0
      val ranked = sorted.map {
        f =>
          rank = rank + 1
          (f._1, rank)
      }
      return ranked
    }

    def computeTermRankChanges(before: Array[(String, Int)], after: Array[(String, Int)]): Array[(String, Int, Int, Int)] = {
      val rankChanges = before.map {
        entry =>
          val term = entry._1
          val rankBefore = entry._2
          val rankAfter = lookupRank(term, after)
          val rankChange = rankBefore - rankAfter
          (term, rankBefore, rankAfter, rankChange)
      }.filter(entry => entry._2 != 0)
      return rankChanges
    }

    def lookupRank(term: String, termRanks: Array[(String, Int)]): Int = {
      termRanks.filter(entry => entry._1 == term).foreach(x => return x._2)
      return 0;
    }

    // tweetsRDD = sc.parallelize(tweetsRDD.takeSample(false, 100000, 4372))
    
    val tweets2012 = tweetsRDD.filter(Post => Post.created_at.contains("2012"))
    val tweets2014 = tweetsRDD.filter(Post => Post.created_at.contains("2014"))
    val topTweetTerms2012 = computeTopTerms(tweets2012, 100)
    val topTweetTerms2014 = computeTopTerms(tweets2014, 100)
    val termRanks2012 = convertOccurencesToRanks(topTweetTerms2012)
    val termRanks2014 = convertOccurencesToRanks(topTweetTerms2014)
    var tweetRankChanges = computeTermRankChanges(termRanks2012, termRanks2014).sortBy(_._4)
    tweetRankChanges = tweetRankChanges.filter(entry => entry._3 != 0)
    println("Moving down:")
    tweetRankChanges.take(10).foreach(println)
    println("Moving up:")
    tweetRankChanges.reverse.take(10).foreach(println)

    // redditRDD = sc.parallelize(redditRDD.takeSample(false, 100000, 4372))

    /*
    val reddit2012 = redditRDD.filter(Post => Post.created_at < 1359529600)
    val reddit2014 = redditRDD.filter(Post => Post.created_at > 1359529600)
    val topTerms2012 = computeTopTerms(reddit2012, 100)
    val topTerms2014 = computeTopTerms(reddit2014, 100)
    val termRanks2012 = convertOccurencesToRanks(topTerms2012)
    val termRanks2014 = convertOccurencesToRanks(topTerms2014)
    var rankChanges = computeTermRankChanges(termRanks2012, termRanks2014).sortBy(_._4)
    rankChanges = rankChanges.filter(entry => entry._3 != 0)
    println("Moving down:")
    rankChanges.take(10).foreach(println)
    println("Moving up:")
    rankChanges.reverse.take(10).foreach(println)
    */

    // redditRDD = sc.parallelize(redditRDD.takeSample(false, 10000, 4372))

    tweetsRDD = tweetsRDD.filter(post => post.text.contains("sopa"))
    redditRDD = redditRDD.filter(post => post.text.contains("sopa"))
    val twitterWordCounts = computeTopTerms(tweetsRDD, 20)
    val redditWordCounts = computeTopTerms(redditRDD, 20)
    
    println("Twitter:")
    twitterWordCounts.foreach(println)
    
    println("Reddit:")
    redditWordCounts.foreach(println)
    
    /*
    def computeUniqueTerms(first: Array[(String, Int)], second: Array[(String, Int)]): Array[(String, Int, Int, Int)] = {
      val uniqueTerms = first.map {
        entry =>
          val term = entry._1
          val rankFirst = entry._2
          val rankSecond = lookupRank(term, second)
          val rankDifference = rankFirst - rankSecond
          (term, rankFirst, rankSecond, rankDifference)
      }.filter(entry => entry._3 == 0)
      return uniqueTerms
    }

    val topRedditTerms = computeTopTerms(redditRDD, 100)
    val topRedditTermsRanks = convertOccurencesToRanks(topRedditTerms)
    
    val topTwitterTerms = computeTopTerms(tweetsRDD, 100)
    val topTwitterTermsRanks = convertOccurencesToRanks(topTwitterTerms)
    
    var topUniqueRedditTerms = computeUniqueTerms(topRedditTermsRanks, topTwitterTermsRanks).sortBy(_._2)
    println("Top Unique Reddit Terms:")
    topUniqueRedditTerms.take(20).foreach(println)
    
    var topUniqueTwitterTerms = computeUniqueTerms(topTwitterTermsRanks, topRedditTermsRanks).sortBy(_._2)
    println("Top Unique Twitter Terms:")
    topUniqueTwitterTerms.take(20).foreach(println)
    */

  }
}