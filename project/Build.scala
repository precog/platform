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
import de.johoop.cpd4sbt.CopyPasteDetector._
import net.virtualvoid.sbt.graph.Plugin.graphSettings

object PlatformBuild extends Build {
  val jprofilerLib = SettingKey[String]("jprofiler-lib", "The library file used by jprofiler")
  val jprofilerConf = SettingKey[String]("jprofiler-conf", "The relative path to jprofiler's XML config file")
  val jprofilerId = SettingKey[String]("jprofiler-id", "The id used to find our session settings in XML")
  val archiveDir = SettingKey[String]("archive-dir", "The temporary directory to which deleted projections will be moved")
  val dataDir = SettingKey[String]("data-dir", "The temporary directory into which to extract the test data")
  val profileTask = InputKey[Unit]("profile", "Runs the given project under JProfiler")
  val extractData = TaskKey[String]("extract-data", "Extracts the data files used by the tests and the REPL")
  val mainTest = SettingKey[String]("main-test", "The primary test class for the project (just used for surtr)")

  val nexusSettings : Seq[Project.Setting[_]] = Seq(
    resolvers ++= Seq(
      "Typesafe Repository"               at "http://repo.typesafe.com/typesafe/releases/",
      "Maven Repo 1"                      at "http://repo1.maven.org/maven2/",
      "Guiceyfruit"                       at "http://guiceyfruit.googlecode.com/svn/repo/releases/",
      "Sonatype Releases"                 at "http://oss.sonatype.org/content/repositories/releases/",
      "Sonatype Snapshots"                at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),

    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )

  val blueeyesVersion = "1.0.0-M9.5"
  val scalazVersion = "7.0.0"

  val commonSettings = Seq(
    organization := "com.precog",
    version := "2.6.0-SNAPSHOT",
    addCompilerPlugin("org.scala-tools.sxr" % "sxr_2.9.0" % "0.2.7"),
    scalacOptions <+= scalaSource in Compile map { "-P:sxr:base-directory:" + _.getAbsolutePath },
    scalacOptions ++= {
      //Seq("-Ywarn-value-discard", "-unchecked", "-g:none") ++
      Seq("-unchecked", "-g:none") ++
      Option(System.getProperty("com.precog.build.optimize")).map { _ => Seq("-optimize") }.getOrElse(Seq())
    },
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    scalaVersion := "2.9.2",

    jarName in assembly <<= (name) map { name => name + "-assembly-" + ("git describe".!!.trim) + ".jar" },
    target in assembly <<= target,

    EclipseKeys.createSrc := EclipseCreateSrc.Default+EclipseCreateSrc.Resource,
    EclipseKeys.withSource := true,
    (unmanagedSourceDirectories in Compile) <<= (scalaSource in Compile, javaSource in Compile)(Seq(_) ++ Set(_)),
    (unmanagedSourceDirectories in Test) <<= (scalaSource in Test)(Seq(_)),

    libraryDependencies ++= Seq(
      "com.weiglewilczek.slf4s"     %  "slf4s_2.9.1"         % "1.0.7",
      "com.google.guava"            %  "guava"              % "13.0",
      "com.google.code.findbugs"    % "jsr305"              % "1.3.+",
      "org.scalaz"                  %% "scalaz-core"        % scalazVersion,
      "org.scalaz"                  %% "scalaz-effect"      % scalazVersion,
      "joda-time"                   %  "joda-time"          % "1.6.2",
      "com.reportgrid"              %% "blueeyes-json"      % blueeyesVersion,
      "com.reportgrid"              %% "blueeyes-util"      % blueeyesVersion,
      "com.reportgrid"              %% "blueeyes-core"      % blueeyesVersion,
      "com.reportgrid"              %% "blueeyes-mongo"     % blueeyesVersion,
      "com.reportgrid"              %% "bkka"               % blueeyesVersion,
      "com.reportgrid"              %% "akka_testing"       % blueeyesVersion,
      "org.scalacheck"              %% "scalacheck"         % "1.10.0" % "test",
      "org.specs2"                  %% "specs2"             % "1.12.3" % "test",
      "org.mockito"                 %  "mockito-core"       % "1.9.0" % "test",
      "javolution"                  %  "javolution"         % "5.5.1",
      "com.chuusai"                 %% "shapeless"          % "1.2.3",
      "org.spire-math"              % "spire_2.9.1"         % "0.3.0-RC2",
      "com.rubiconproject.oss"      % "jchronic"            % "0.2.6",
      "javax.servlet"               % "servlet-api"         % "2.4" % "provided"
    )
  )

  val jettySettings = Seq(
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server"      % "8.1.3.v20120416",
      "javax.servlet"     % "javax.servlet-api"   % "3.0.1"
    ),
    ivyXML :=
    <dependencies>
      <dependency org="org.eclipse.jetty" name="jetty-server" rev="8.1.3.v20120416">
        <exclude org="org.eclipse.jetty.orbit" />
      </dependency>
    </dependencies>
  )

  val jprofilerSettings = Seq(
    fork in profileTask := true,
    fork in run := true,

    jprofilerLib := "/Applications/jprofiler7/bin/macos/libjprofilerti.jnilib",
    jprofilerConf := "src/main/resources/jprofile.xml",
    jprofilerId := "116",

    javaOptions in profileTask <<= (javaOptions, jprofilerLib, jprofilerConf, jprofilerId, baseDirectory) map {
      (opts, lib, conf, id, d) =>
      // download jnilib if necessary. a bit sketchy, but convenient
      Process("./jprofiler/setup-jnilib.py").!!
      opts ++ Seq("-agentpath:%s/jprofiler.jnilib=offline,config=%s/%s,id=%s" format (d, d, conf, id))
    }
  )

  val commonPluginsSettings = ScctPlugin.instrumentSettings ++ cpdSettings ++ graphSettings ++ commonSettings
  val commonNexusSettings = nexusSettings ++ commonPluginsSettings
  val commonAssemblySettings = sbtassembly.Plugin.assemblySettings ++ Seq(test in assembly := {}) ++ commonNexusSettings

  // Logging is simply a common project for the test log configuration files
  lazy val logging = Project(id = "logging", base = file("logging")).settings(commonNexusSettings: _*)

  lazy val standalone = Project(id = "standalone", base = file("standalone")).
    settings((commonAssemblySettings  ++ jettySettings): _*) dependsOn(common % "compile->compile;test->test", yggdrasil % "compile->compile;test->test", util, bifrost, muspelheim % "compile->compile;test->test", logging % "test->test", auth, accounts, ingest, dvergr)

  lazy val platform = Project(id = "platform", base = file(".")).
    settings(ScctPlugin.mergeReportSettings ++ ScctPlugin.instrumentSettings: _*).
    aggregate(quirrel, mirror, yggdrasil, bytecode, mimir, ingest, bifrost, auth, accounts, surtr, util, common, ragnarok , dvergr, ratatoskr) //, mongo, jdbc, desktop)

  lazy val util = Project(id = "util", base = file("util")).
    settings(commonNexusSettings: _*) dependsOn(logging % "test->test")

  lazy val common = Project(id = "common", base = file("common")).
    settings(commonNexusSettings: _*) dependsOn (util, logging % "test->test")

  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).
    settings(commonNexusSettings: _*) dependsOn(logging % "test->test")

  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).
    settings(commonNexusSettings: _*) dependsOn (bytecode % "compile->compile;test->test", util, logging % "test->test")

  lazy val mirror = Project(id = "mirror", base = file("mirror")).
    settings(commonNexusSettings: _*) dependsOn (quirrel)

  lazy val niflheim = Project(id = "niflheim", base = file("niflheim")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", util, logging % "test->test")

  lazy val yggdrasil = Project(id = "yggdrasil", base = file("yggdrasil")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", bytecode, util, niflheim, logging % "test->test")

  lazy val yggdrasilProf = Project(id = "yggdrasilProf", base = file("yggdrasilProf")).
    settings(commonNexusSettings ++ jprofilerSettings ++ Seq(fullRunInputTask(profileTask, Test, "com.precog.yggdrasil.test.Run")): _*).dependsOn(yggdrasil % "compile->compile;compile->test", logging % "test->test")

  lazy val mongo = Project(id = "mongo", base = file("mongo")).
    settings(commonAssemblySettings: _*).dependsOn(standalone, muspelheim % "compile->compile;test->test")

  lazy val jdbc = Project(id = "jdbc", base = file("jdbc")).
    settings(commonAssemblySettings: _*).dependsOn(standalone, muspelheim % "compile->compile;test->test")

  lazy val desktop = Project(id = "desktop", base = file("desktop")).
    settings(commonAssemblySettings: _*).dependsOn(standalone, bifrost)

  lazy val mimir = Project(id = "mimir", base = file("mimir")).
    settings(commonNexusSettings: _*).dependsOn (util % "compile->compile;test->test", common, bytecode % "compile->compile;test->test", yggdrasil % "compile->compile;test->test", logging % "test->test")

  /// Testing ///

  lazy val muspelheim = Project(id = "muspelheim", base = file("muspelheim")).
    settings(commonNexusSettings: _*) dependsOn (util % "compile->compile;test->test", common, quirrel, mimir, yggdrasil % "compile->compile;test->test", logging % "test->test")

  lazy val surtr = Project(id = "surtr", base = file("surtr")).
    settings(commonAssemblySettings: _*) dependsOn (quirrel, mimir, yggdrasil, ingest, muspelheim % "compile->compile;test->test", logging % "test->test")

  lazy val ragnarok = Project(id = "ragnarok", base = file("ragnarok")).
    settings(commonAssemblySettings: _*).dependsOn(quirrel, mimir, yggdrasil, ingest, muspelheim % "compile->compile;test->test", logging % "test->test")

  lazy val gjallerhorn = Project(id = "gjallerhorn", base = file("gjallerhorn")).
    settings(commonAssemblySettings: _*).dependsOn(quirrel, mimir, yggdrasil % "compile->test", ingest, muspelheim % "compile->compile;test->test", logging % "test->test")

  lazy val performance = Project(id = "performance", base = file("performance")).
    settings(commonNexusSettings: _*).dependsOn(ingest, common % "compile->compile;test->test", quirrel, mimir, yggdrasil, bifrost, logging % "test->test")

  lazy val jprofiler = Project(id = "jprofiler", base = file("jprofiler")).
    settings(jprofilerSettings ++ commonNexusSettings ++ Seq(fullRunInputTask(profileTask, Test, "com.precog.jprofiler.Run")): _*).dependsOn(ragnarok, logging % "test->test")

  /// Services ///

  lazy val auth = Project(id = "auth", base = file("auth")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", logging % "test->test")

  lazy val accounts     = Project(id = "accounts", base = file("accounts")).
    settings(commonAssemblySettings: _*) dependsOn (common % "compile->compile;test->test", auth, logging % "test->test")

  lazy val ingest = Project(id = "ingest", base = file("ingest")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", yggdrasil, logging % "test->test")

  lazy val dvergr = Project(id = "dvergr", base = file("dvergr")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", util, logging % "test->test")

  lazy val bifrost = Project(id = "bifrost", base = file("bifrost")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", muspelheim, surtr % "test->test")

  /// Tooling ///

  lazy val ratatoskr = Project(id = "ratatoskr", base = file("ratatoskr")).
    settings(commonAssemblySettings: _*).dependsOn(mimir, ingest, auth, accounts)
}
