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
name := "quirrel"

organization := "com.querio"

version := "0.1.0"

scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "edu.uwm.cs" %% "gll-combinators" % "1.5-SNAPSHOT" changing(),
  "org.scalaz" %% "scalaz-core" % "7.0-SNAPSHOT" changing(),
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources() changing(),
  "org.specs2" %% "specs2" % "1.8-SNAPSHOT" % "test" withSources() changing())
  
initialCommands in console := """
  | import edu.uwm.cs.gll.LineStream
  | 
  | import com.querio.quirrel._
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
