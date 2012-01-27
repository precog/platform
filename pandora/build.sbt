name := "pandora"

version := "0.0.1-SNAPSHOT"

organization := "com.quirio"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "jline" % "jline" % "0.9.9",
  "org.specs2" %% "specs2" % "1.8-SNAPSHOT" % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9")
  
logBuffered := false       // gives us incremental output from Specs2
