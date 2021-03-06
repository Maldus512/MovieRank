name := "MovieRank"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"
libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4" 
//libraryDependencies += "com.acme.common" % "commonclass" % "1.0" from "file:///Users/bwong/git/perf-tools/commonclass/target/scala-2.11/commonclass_2.11-1.0.jar

unmanagedJars in Compile += file("lib/aws-java-sdk-1.7.4.jar")
unmanagedJars in Compile += file("lib/hadoop-aws-2.7.2.jar")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
