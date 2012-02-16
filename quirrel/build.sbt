name := "quirrel"

organization := "com.precog"

version := "0.1.0"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "edu.uwm.cs" %% "gll-combinators" % "1.5-SNAPSHOT" changing(),
  "org.scalaz" %% "scalaz-core" % "7.0-SNAPSHOT" changing(),
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources() changing(),
  "org.specs2" %% "specs2" % "1.8" % "test" withSources())
  
initialCommands in console := """
  | import edu.uwm.cs.gll.LineStream
  | 
  | import com.precog.quirrel._
  | import emitter._
  | import parser._
  | import typer._
  |
  | val compiler = new Parser with Binder with ProvenanceChecker with CriticalConditionSolver with Compiler with Emitter with LineErrors {}
  | 
  | trait StubPhases extends Phases with RawErrors {
  |   def bindNames(tree: Expr) = Set()
  |   def checkProvenance(tree: Expr) = Set()
  |   def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
  |   def solveCriticalConditions(expr: Expr) = Set()
  | }
  | 
  | val solver = new Solver with Parser with StubPhases {}
  |
  | def solve(str: String, id: Symbol): Option[solver.Expr] = {
  |   import solver.ast._
  |   val f = solver.solve(solver.parse(LineStream(str))) { case TicVar(_, id2) => id.toString == id2 }
  |   f(NumLit(LineStream(), "0"))
  | }
  """.stripMargin
  
logBuffered := false       // gives us incremental output from Specs2
