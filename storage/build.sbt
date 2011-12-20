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
import AssemblyKeys._

name := "leveldb"

version := "0.0.1-SNAPSHOT"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

fork := true

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

libraryDependencies ++= Seq(
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0-M1",
  "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
  "org.apache"                  %% "kafka-core"         % "0.7.5",
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.1-SNAPSHOT",
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.1-SNAPSHOT",
  "org.scala-tools.testing"     %% "scalacheck"         % "1.9",
  "org.specs2"                  %% "specs2"             % "1.7-SNAPSHOT"   % "test"
)

resolvers ++= Seq(
  "Local Maven Repository" at     "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Typesafe Repository" at        "http://repo.typesafe.com/typesafe/releases/",
  "Scala-Tools Releases" at       "http://scala-tools.org/repo-releases/",
  "Scala-Tools Snapshots" at      "http://scala-tools.org/repo-snapshots/",
  "Nexus Scala Tools" at          "http://nexus.scala-tools.org/content/repositories/releases",
  "Maven Repo 1" at               "http://repo1.maven.org/maven2/"
)

seq(assemblySettings: _*)
