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
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object SharedSettings {
  val buildOrganization = "com.reportgrid"
  val buildScalaVersion = "2.9.1"
  
  val sharedSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    scalaVersion := buildScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    resolvers += "Scala-Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"
  )
}

object PlatformBuild extends Build {
  import SharedSettings._
  
  val nexusSettings = Seq(
    resolvers ++= Seq("ReportGrid repo" at            "http://devci01.reportgrid.com:8081/content/repositories/releases",
                      "ReportGrid snapshot repo" at   "http://devci01.reportgrid.com:8081/content/repositories/snapshots"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )
  
  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel, storage, bytecode, daze, ingest)
  
  lazy val blueeyes = RootProject(uri("../blueeyes"))
  lazy val clientLibraries = RootProject(uri("../client-libraries/scala"))

  
  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(nexusSettings : _*)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).settings(nexusSettings : _*) dependsOn bytecode
  lazy val storage = Project(id = "storage", base = file("storage")).settings(nexusSettings : _*)
  
  lazy val daze = Project(id = "daze", base = file("daze")).settings(nexusSettings : _*) dependsOn bytecode // (bytecode, storage)
  
  val commonSettings = sharedSettings ++ Seq( 
      version      := "0.1.0-SNAPSHOT",
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"           % "1.6.2",
        "org.scalaz"              %% "scalaz-core"        % "6.0.2",
        "org.specs2"              %% "specs2"             % "1.7-SNAPSHOT"  % "test"
      )
  )

  lazy val common = Project(id = "common", base = file("common"), settings = sbtassembly.Plugin.assemblySettings ++ commonSettings) dependsOn(blueeyes)

  val ingestSettings = sharedSettings ++ Seq( 
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      "joda-time"               % "joda-time"           % "1.6.2",
      "ch.qos.logback"          % "logback-classic"     % "1.0.0",
      "org.scalaz"              %% "scalaz-core"        % "6.0.2",
      "org.specs2"              %% "specs2"             % "1.7-SNAPSHOT"  % "test",
      "org.scala-tools.testing" %% "scalacheck"         % "1.9"    % "test"
    ),
    mainClass := Some("com.querio.ingest.IngestServer"), 
    parallelExecution in Test := false,
    test in assembly := {}
  )

  lazy val ingest = Project(id = "ingest", base = file("ingest"), settings = sbtassembly.Plugin.assemblySettings ++ ingestSettings) dependsOn(common) dependsOn(blueeyes) dependsOn(clientLibraries)
}

