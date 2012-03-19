name := "quirrel"

organization := "com.precog"

version := "0.1.0"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "com.codecommit" %% "gll-combinators" % "2.0",
  "org.scalaz" %% "scalaz-core" % "7.0-SNAPSHOT" changing(),
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources() changing(),
  "org.specs2" %% "specs2" % "1.8" % "test" withSources())
  
initialCommands in console := """
  | import com.codecommit.gll.LineStream
  | 
  | import com.precog.quirrel._
  | import emitter._
  | import parser._
  | import typer._
  | import QuirrelConsole._
  """.stripMargin
  
logBuffered := false       // gives us incremental output from Specs2
