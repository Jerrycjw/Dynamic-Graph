
name := "untitled5"

version := "1.0"

libraryDependencies ++= Seq( "org.twitter4j" % "twitter4j-core" % "3.0.3" , "org.twitter4j" % "twitter4j-stream" % "3.0.3",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.1.0" )

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*)  => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

