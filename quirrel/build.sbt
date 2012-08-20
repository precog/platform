name := "quirrel"

libraryDependencies ++= Seq(
  "com.codecommit" %% "gll-combinators" % "2.2-SNAPSHOT"
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
