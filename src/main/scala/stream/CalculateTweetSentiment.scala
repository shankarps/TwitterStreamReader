package stream

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import utilities.Utilities._


/** Listens to a stream of tweets, calculate the affinity (sentiment) of the words in a batch and print them for each batch. */
object CalculateTweetSentiment {

  System.setProperty(
    "spark.sql.warehouse.dir", 
    s"file:///${System.getProperty("user.dir")}/spark-warehouse"
    .replaceAll("\\\\", "/")
)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    val sparkConf = new SparkConf().setAppName("StreamTwitter")
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
    
    //TO DEBUG
    //affinText.printSchema()
    //affinText.foreach(println)
 
    
    // Set up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and 60-second batches of data
    val ssc = new StreamingContext(sc, Seconds(60))
    
    // Setup log level.
    setupLogging()

    //Define the filters
    val arr = Array("Rogue One", "#RogueOne")
    
   
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
        
      //AffinityCount.show()
        
      AffinityCount.createOrReplaceTempView("AffinityCount")
         
      //TO DEBUG
      //AffinityCount.printSchema()
         
      val negativeAffinity =
        sqlContext.sql("select sum(product) from AffinityCount where product < 0")

      val positiveAffinity =
        sqlContext.sql("select sum(product) from AffinityCount where product >= 0")

      //TO DEBUG  
      //totalAffinity.show()
        
      println(""+time+" ms : Negative Affinity: " + negativeAffinity.head().getLong(0)+" - Positive Affinity: " + positiveAffinity.head().getLong(0))
        
    })
      
    
   
    // To keep count of how many Tweets we've received
    var totalTweets:Long = 0
        
    statuses.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        //val repartitionedRDD = rdd.cache()
        // To debug print tweets to a directory.
        //repartitionedRDD.saveAsTextFile("data/Tweets_" + time.milliseconds.toString)

        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        //Stop the process if total has exceeded.
        if (totalTweets > 5000) {
          System.exit(0)
        }
      }
    })
    
    // Set a checkpoint directory, and start.
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }  
}
/** Case class for converting RDD to DataFrame */
case class AffinRecord(word: String, affin: Int)

case class TweetWords(tweetWord: String, count: Int)
