package stream

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import utilities.Utilities._


/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets {

  System.setProperty(
    "spark.sql.warehouse.dir", 
    s"file:///${System.getProperty("user.dir")}/spark-warehouse"
    .replaceAll("\\\\", "/")
)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    val sparkConf = new SparkConf().setAppName("Elections")
    sparkConf.setMaster("local[*]")
       
    val sc = new SparkContext(sparkConf)
    
    //Load the AFINN text
    val affinWords = sc.textFile("assets/AFINN-emoticon-8.txt").map(x => {val arr = x.split("\t"); (arr(0), arr(1).toInt)})
    
    val affinSmiley = sc.textFile("assets/AFINN-en-165.txt").map(x => {val arr = x.split("\t"); (arr(0), arr(1).toInt)})
      
    // Get the singleton instance of SQLContext
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._
      
    val affinText = affinWords.union(affinSmiley).map(w => AffinRecord(w._1, w._2)).toDF();
    affinText.createOrReplaceTempView("affinity")
    
    //affinText.printSchema()
    
    //affinText.foreach(println)
 
    
    // Set up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and 60-second batches of data
    val ssc = new StreamingContext(sc, Seconds(10))
    
    // Setup log level.
    setupLogging()

    //Define the filters
    val arr = Array("India")
    
   
    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None, arr)
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(tweet => tweet.getText())
    
    //convert to a flatMap
    val statusWords = statuses.flatMap { line => line.split(" ") }.map(word => (word, 1)).reduceByKey(_ + _)
    
    statusWords.foreachRDD((words, time) => {
      //words.foreach(print)
      val wordMatches = words.map(w => TweetWords(w._1, w._2)).toDF();
      
        wordMatches.createOrReplaceTempView("wordCount")
        
      val AffinityCount =
        sqlContext.sql("select word as words, (affin * count) as product from wordCount, affinity where affinity.word = wordCount.tweetWord")
        
        AffinityCount.show()
        
         AffinityCount.createOrReplaceTempView("AffinityCount")
         
         //AffinityCount.printSchema()
         
        val totalAffinity =
        sqlContext.sql("select sum(product) from AffinityCount")
        
        totalAffinity.show()
        
        println("Affinity count: " + totalAffinity)
        
    })
      
    
   
    // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
    var totalTweets:Long = 0
        
    statuses.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        //val repartitionedRDD = rdd.cache()
        // And print out a directory with the results.
        repartitionedRDD.saveAsTextFile("data/Tweets_" + time.milliseconds.toString)
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 50) {
          System.exit(0)
        }
      }
    })
    
    // You can also write results into a database of your choosing, but we'll do that later.
    
    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("D:/Spark/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
/** Case class for converting RDD to DataFrame */
case class AffinRecord(word: String, affin: Int)

case class TweetWords(tweetWord: String, count: Int)
