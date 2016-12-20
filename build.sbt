name := "Twitter Stream Project"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0"



addCommandAlias("c1", "run-main stream.SaveTweets")


outputStrategy := Some(StdoutOutput)
//outputStrategy := Some(LoggedOutput(log: Logger))

fork in run := true