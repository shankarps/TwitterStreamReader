# Twitter Sentiment Meter

    This test project project measures the real time sentiment (positive or negative) of a given topic from tweets streamed from Twittter.
    It is a Spark Streaming API Scala project that streams tweets from Twitter. 
    It was inspired from this static Twitter sentiment analysis described here - https://acadgild.com/blog/pig-use-case-sentiment-analysis-on-demonetization/.
    

##Acknowledgement

    I have used the AFINN words list from this repository - https://github.com/fnielsen/afinn/tree/master/afinn
    I have referred to this Udemy course for learning about Spark Streaming API, Twitter Streaming API and Scala sample code 
    	- https://www.udemy.com/taming-big-data-with-spark-streaming-hands-on/learn/v4/overview

##Prerequisites

    1. Scala IDE
    2. SBT
    3. Twitter API credentials and a Twitter App
    
##Project Setup

    1. From Scala IDE clone the git URI and setup a new project.
    2. Create a file "twitter.txt" in project base folder and fill the following values.
			consumerKey 
			consumerSecret 
			accessToken 
			accessTokenSecret 
    3. Navigate to command prompt to the project folder and run 
    	$ >sbt "run-main <PACKAGE>.<CLASS NAME>"

##How it works
    
    1. When the class CalculateTweetSentiment is launched, the Spark Context loads the given AFFIN words dictionary into an RDD  
    2. The Spark Streaming Twitter Util gets tweets that match the filters into the Streaming Context for a given batch window.
    3. For every batch of tweets, convert the tweets' text into a flat map of words and ther count. For example, "Hello, how are you?" becomes ((Hello,1),(how,1),(are,1),(you,1))
    4. Join the tweet words with the AFFINN words. This will eliminate the non-AFINN words (for example, "a", "under", "below", proper nouns, etc) 
       and we will have the AFINN words, their count and valence.
    5. For each word, multiply the word count and the AFINN valence.
    6. Print the total valence (sentiment) and the total number of tweets for each batch and proceed to next batch. 
    
###References
  1. http://www2.imm.dtu.dk/pubdb/views/publication_details.php?id=6010
  2. http://stackoverflow.com/questions/23617920/why-is-the-error-conflicting-cross-version-suffixes
  3. http://ampcamp.berkeley.edu/3/exercises/realtime-processing-with-spark-streaming.html
  4. http://stackoverflow.com/questions/38893655/spark-twitter-streaming-exception-org-apache-spark-logging-classnotfound/39194820#39194820
  