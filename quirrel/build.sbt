name := "quirrel"

version := "0.1.0"

libraryDependencies ++= Seq(
  "com.codecommit" % "gll-combinators_2.9.1" % "2.0"
)
  
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
