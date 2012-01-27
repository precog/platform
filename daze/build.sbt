name := "daze"

version := "0.0.1-SNAPSHOT"

organization := "com.quirio"

scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "1.7" % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9")
  
logBuffered := false       // gives us incremental output from Specs2
