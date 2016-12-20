/**
 * Get Tweets and pring the number of tweets for each batch.
 */
package stream

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import utilities.Utilities._


object PrintTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark context named "StreamTwitter" that runs locally using
    // all CPU cores and 60-second batches of data
    val sparkConf = new SparkConf().setAppName("StreamTwitter")
    sparkConf.setMaster("local[*]")
       
    val sc = new SparkContext(sparkConf)
    
    // Get the singleton instance of SQLContext
      val sqlContext = new SQLContext(sc);
    
    // Set up a Spark streaming context
    val ssc = new StreamingContext(sc, Seconds(60))
    
    // Setup log level.
    setupLogging()

    //Define the filters
    val arr = Array("India")
    
   
    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None, arr)
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(tweet => tweet.getText())
    
    // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
    var totalTweets:Long = 0
        
    statuses.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()

        // Uncomment to print out a directory with the results.
        //repartitionedRDD.saveAsTextFile("data/Tweets_" + time.milliseconds.toString)
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)

        // Stop once we've collected 1000 tweets.
        if (totalTweets > 5000) {
          System.exit(0)
        }
      }
    })
    
    // Set a checkpoint directory, and start.
    ssc.checkpoint("D:/Spark/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}

