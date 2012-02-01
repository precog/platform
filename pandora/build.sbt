name := "pandora"

version := "0.0.1-SNAPSHOT"

organization := "com.quirio"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.sonatype.jline" % "jline" % "2.5",
  "org.specs2" %% "specs2" % "1.7" % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9")
  
outputStrategy := Some(StdoutOutput)

connectInput in run := true
  
fork in run := true

fork in console := true
  
initialCommands in console := """
  | import edu.uwm.cs.gll.LineStream
  | 
  | import com.precog._
  |
  | import daze._
  | import daze.util._
  | 
  | import quirrel._
  | import quirrel.emitter._
  | import quirrel.parser._
  | import quirrel.typer._
  |
  | val platform = new Parser
  |                  with Binder
  |                  with ProvenanceChecker
  |                  with CriticalConditionSolver
  |                  with Compiler
  |                  with Emitter
  |                  with Evaluator
  |                  with YggdrasilOperationsAPI
  |                  with DefaultYggConfig
  |                  with StubQueryAPI
  |                  with DAGPrinter
  |                  with LineErrors {}""".stripMargin
  
logBuffered := false       // gives us incremental output from Specs2
