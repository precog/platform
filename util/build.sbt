name := "util"

organization := "com.precog"

version := "0.1.0"

scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "org.scalaz"                  %% "scalaz-core"        % "7.0-SNAPSHOT"                changing(),
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.specs2" %% "specs2" % "1.8" % "test" withSources())
  
logBuffered := false       // gives us incremental output from Specs2
