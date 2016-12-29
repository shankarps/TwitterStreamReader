package utilities

import java.util.regex.Pattern

object Utilities {
    /** Log only ERROR messages */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter API credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
}