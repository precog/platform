scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "jline" % "jline" % "0.9.9",
  "edu.uwm.cs" %% "gll-combinators" % "1.5-SNAPSHOT",
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.specs2" %% "specs2" % "1.7-SNAPSHOT" % "test" withSources)
  
initialCommands := """
  | import edu.uwm.cs.gll.LineStream
  | 
  | import com.reportgrid.quirrel._
  | import parser._
  | import typer._
  |
  | val compiler = new Parser with Binder with ProvenanceChecker with LineErrors {}
  | 
  | trait StubPhases extends Phases with RawErrors {
  |   def bindNames(tree: Expr) = Set()
  |   def checkProvenance(tree: Expr) = Set()
  | }
  | 
  | val solver = new Solver with Parser with StubPhases {}
  |
  | def solve(str: String, id: Symbol): Option[solver.Expr] = {
  |   val f = solver.solve(solver.parse(LineStream(str))) { case solver.TicVar(_, id2) => id.toString == id2 }
  |   f(solver.NumLit(LineStream(), "0"))
  | }
  """.stripMargin
  
logBuffered := false       // gives us incremental output from Specs2
