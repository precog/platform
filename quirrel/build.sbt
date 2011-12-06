/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "jline" % "jline" % "0.9.9",
  "edu.uwm.cs" %% "gll-combinators" % "1.5-SNAPSHOT",
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.specs2" %% "specs2" % "1.7-SNAPSHOT" % "test" withSources)
  
initialCommands in console := """
  | import edu.uwm.cs.gll.LineStream
  | 
  | import com.reportgrid.quirrel._
  | import parser._
  | import typer._
  |
  | val compiler = new Parser with Binder with ProvenanceChecker with CriticalConditionFinder with Compiler with LineErrors {}
  | 
  | trait StubPhases extends Phases with RawErrors {
  |   def bindNames(tree: Expr) = Set()
  |   def checkProvenance(tree: Expr) = Set()
  |   def findCriticalConditions(expr: Expr): Map[String, Set[Expr]] = Map()
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
