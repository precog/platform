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
import com.typesafe.sbteclipse.plugin.EclipsePlugin.{ EclipseKeys, EclipseCreateSrc }

object PlatformBuild extends Build {
  val jprofilerLib = SettingKey[String]("jprofiler-lib", "The library file used by jprofiler")
  val jprofilerConf = SettingKey[String]("jprofiler-conf", "The relative path to jprofiler's XML config file")
  val jprofilerId = SettingKey[String]("jprofiler-id", "The id used to find our session settings in XML")
  val archiveDir = SettingKey[String]("archive-dir", "The temporary directory to which deleted projections will be moved")
  val dataDir = SettingKey[String]("data-dir", "The temporary directory into which to extract the test data")
  val profileTask = InputKey[Unit]("profile", "Runs the given project under JProfiler")
  val extractData = TaskKey[String]("extract-data", "Extracts the data files used by the tests and the REPL")
  val mainTest = SettingKey[String]("main-test", "The primary test class for the project (just used for pandora)")

  val nexusSettings : Seq[Project.Setting[_]] = Seq(
    resolvers ++= Seq(
      "ReportGrid repo"                   at "http://nexus.reportgrid.com/content/repositories/releases",
      "ReportGrid repo (public)"          at "http://nexus.reportgrid.com/content/repositories/public-releases",
      "ReportGrid snapshot repo"          at "http://nexus.reportgrid.com/content/repositories/snapshots",
      "ReportGrid snapshot repo (public)" at "http://nexus.reportgrid.com/content/repositories/public-snapshots",
      "Typesafe Repository"               at "http://repo.typesafe.com/typesafe/releases/",
      "Maven Repo 1"                      at "http://repo1.maven.org/maven2/",
      "Guiceyfruit"                       at "http://guiceyfruit.googlecode.com/svn/repo/releases/",
      "Sonatype Snapshots"                at "http://oss.sonatype.org/content/repositories/releases/",
      "Sonatype Snapshots"                at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),

    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials"),

    publishTo <<= (version) { version: String =>
      val nexus = "http://nexus.reportgrid.com/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/") 
      else                                   Some("releases"  at nexus+"releases/")
    }
  )

  val commonSettings = Seq(
    organization := "com.precog",
    version := "2.1-SNAPSHOT",
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    scalaVersion := "2.9.2",

    defaultJarName in assembly <<= (name) { name => name + "-assembly-" + ("git describe".!!.trim) + ".jar" },

    EclipseKeys.createSrc := EclipseCreateSrc.Default+EclipseCreateSrc.Resource,
    EclipseKeys.withSource := true,
    (unmanagedSourceDirectories in Compile) <<= (scalaSource in Compile, javaSource in Compile)(Seq(_) ++ Set(_)),
    (unmanagedSourceDirectories in Test) <<= (scalaSource in Test)(Seq(_)),

    libraryDependencies ++= Seq(
      "com.weiglewilczek.slf4s"     % "slf4s_2.9.1"         % "1.0.7",
      "org.scalaz"                  %% "scalaz-core"        % "7.0-SNAPSHOT" changing(),
      "org.scalaz"                  %% "scalaz-effect"      % "7.0-SNAPSHOT" changing(),
      "org.scalacheck"              %% "scalacheck"         % "1.10.0" % "test",
      "org.specs2"                  %% "specs2"             % "1.12.2" % "test",
      "org.mockito"                 %  "mockito-core"       % "1.9.0" % "test",
      "javolution"                  %  "javolution"         % "5.5.1"//,
      //"org.apache.lucene"           %  "lucene-core"        % "3.6.1"
    )
  )

  val jprofilerSettings = Seq(
    fork in profileTask := true,
    fork in run := true,

    jprofilerLib := "/Applications/jprofiler7/bin/macos/libjprofilerti.jnilib",
    jprofilerConf := "src/main/resources/jprofile.xml",
    jprofilerId := "116",
    
    javaOptions in profileTask <<= (javaOptions, jprofilerLib, jprofilerConf, jprofilerId, baseDirectory) {
      (opts, lib, conf, id, d) =>
      // download jnilib if necessary. a bit sketchy, but convenient
      Process("./jprofiler/setup-jnilib.py").!!
      opts ++ Seq("-agentpath:%s/jprofiler.jnilib=offline,config=%s/%s,id=%s" format (d, d, conf, id))
    }
  )

  val commonNexusSettings = nexusSettings ++ commonSettings
  val commonAssemblySettings = sbtassembly.Plugin.assemblySettings ++ commonNexusSettings

  lazy val platform = Project(id = "platform", base = file(".")).
    aggregate(quirrel, yggdrasil, bytecode, daze, ingest, shard, auth, pandora, util, common, ragnarok)

  lazy val util = Project(id = "util", base = file("util")).
    settings(commonNexusSettings: _*)

  lazy val common = Project(id = "common", base = file("common")).
    settings(commonNexusSettings: _*) dependsOn (util)

  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).
    settings(commonNexusSettings: _*)

  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).
    settings(commonNexusSettings: _*) dependsOn (bytecode % "compile->compile;test->test", util)

  lazy val yggdrasil = Project(id = "yggdrasil", base = file("yggdrasil")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", bytecode, util)

  lazy val yggdrasilProf = Project(id = "yggdrasilProf", base = file("yggdrasilProf")).
    settings(commonNexusSettings ++ jprofilerSettings ++ Seq(fullRunInputTask(profileTask, Test, "com.precog.yggdrasil.test.Run")): _*).dependsOn(yggdrasil % "compile->compile;compile->test")

  lazy val mongo = Project(id = "mongo", base = file("mongo")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", yggdrasil % "compile->compile;test->test", util)

  lazy val daze = Project(id = "daze", base = file("daze")).
    settings(commonNexusSettings: _*).dependsOn (common, bytecode % "compile->compile;test->test", yggdrasil % "compile->compile;test->test", util)

  lazy val muspelheim = Project(id = "muspelheim", base = file("muspelheim")).
    settings(commonNexusSettings: _*) dependsOn (common, quirrel, daze, yggdrasil % "compile->compile;test->test")

  lazy val pandora = Project(id = "pandora", base = file("pandora")).
    settings(commonAssemblySettings: _*) dependsOn (quirrel, daze, yggdrasil, ingest, muspelheim % "compile->compile;test->test")

  lazy val ingest = Project(id = "ingest", base = file("ingest")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", quirrel, daze, yggdrasil)

  lazy val shard = Project(id = "shard", base = file("shard")).
    settings(commonAssemblySettings: _*).dependsOn(ingest, common % "compile->compile;test->test", quirrel, daze, yggdrasil, pandora, muspelheim % "test->test")

  lazy val auth = Project(id = "auth", base = file("auth")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test")

  lazy val accounts     = Project(id = "accounts", base = file("accounts")).
    settings(commonAssemblySettings: _*) dependsOn (common % "compile->compile;test->test", auth, common)
 
  lazy val performance = Project(id = "performance", base = file("performance")).
    settings(commonNexusSettings: _*).dependsOn(ingest, common % "compile->compile;test->test", quirrel, daze, yggdrasil, shard)

  lazy val ragnarok = Project(id = "ragnarok", base = file("ragnarok")).
    settings(commonAssemblySettings: _*).dependsOn(quirrel, daze, yggdrasil, ingest, muspelheim % "compile->compile;test->test")

  lazy val jprofiler = Project(id = "jprofiler", base = file("jprofiler")).
    settings(jprofilerSettings ++ commonNexusSettings ++ Seq(fullRunInputTask(profileTask, Test, "com.precog.jprofiler.Run")): _*).dependsOn(ragnarok)
}
