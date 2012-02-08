name := "daze"

version := "0.0.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "1.8-SNAPSHOT" % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9")
  
logBuffered := false       // gives us incremental output from Specs2
