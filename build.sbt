name := "Twitter Stream Project"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

addCommandAlias("c1", "run-main stream.AverageTweetLength")


outputStrategy := Some(StdoutOutput)
//outputStrategy := Some(LoggedOutput(log: Logger))

fork in run := true