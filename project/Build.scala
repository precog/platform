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
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object PlatformBuild extends Build {
  val condeps = com.samskivert.condep.Depends(
    ("scalaz", "iteratee", "org.scalaz"                  %% "scalaz-iteratee"        % "7.0-SNAPSHOT")
  )

  val commonSettings = Seq(
    resolvers ++= Seq("ReportGrid repo" at            "http://devci01.reportgrid.com:8081/content/repositories/releases",
                      "ReportGrid snapshot repo" at   "http://devci01.reportgrid.com:8081/content/repositories/snapshots"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )
  // Use RG Nexus instance
  lazy val platform = Project(id = "platform", base = file(".")).aggregate(quirrel, storage, bytecode)
  
  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(commonSettings :_*)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).dependsOn(bytecode).settings(commonSettings :_*)
  
  lazy val storage = condeps.addDeps(Project(
    "storage", file("storage/leveldbstore"), 
    settings = Defaults.defaultSettings ++ commonSettings ++ Seq(
      version := "0.0.1-SNAPSHOT",
      organization := "com.reportgrid",
      scalaVersion := "2.9.1",
      scalacOptions ++= Seq("-deprecation", "-unchecked"),
      fork := true,
      // For now, skip column specs because SBT will die a horrible, horrible death
      testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec"))),

      libraryDependencies ++= Seq(
        "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.1-SNAPSHOT",
        "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.1-SNAPSHOT",
        "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
        "org.specs2"                  %% "specs2"             % "1.7-SNAPSHOT"   % "test",
        "org.scala-tools.testing"     %% "scalacheck"         % "1.9",
        "se.scalablesolutions.akka"   %  "akka-actor"         % "1.2",
        "ch.qos.logback"              %  "logback-classic"    % "1.0.0"
      ),

      resolvers ++= Seq(
        "Local Maven Repository" at     "file://"+Path.userHome.absolutePath+"/.m2/repository",
        "Scala-Tools Releases" at       "http://scala-tools.org/repo-releases/",
        "Scala-Tools Snapshots" at      "http://scala-tools.org/repo-snapshots/",
        "Akka Repository" at            "http://akka.io/repository/",
        "Nexus Scala Tools" at          "http://nexus.scala-tools.org/content/repositories/releases",
        "Maven Repo 1" at               "http://repo1.maven.org/maven2/"
      )
    ) ++ seq(assemblySettings: _*)
  ))
}

