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
      "Sonatype Releases"                 at "http://oss.sonatype.org/content/repositories/releases/",
      "Sonatype Snapshots"                at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),

    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials"),

    publishTo <<= (version) { version: String =>
      val nexus = "http://nexus.reportgrid.com/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/")
      else                                   Some("releases"  at nexus+"releases/")
    }
  )

  val blueeyesVersion = "1.0.0-M9"
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
    settings((commonAssemblySettings  ++ jettySettings): _*) dependsOn(common % "compile->compile;test->test", yggdrasil % "compile->compile;test->test", util, shard, muspelheim % "compile->compile;test->test", logging % "test->test", auth, accounts, ingest, heimdall)

  lazy val platform = Project(id = "platform", base = file(".")).
    settings(ScctPlugin.mergeReportSettings ++ ScctPlugin.instrumentSettings: _*).
    aggregate(quirrel, yggdrasil, bytecode, daze, ingest, shard, auth, accounts, pandora, util, common, ragnarok , heimdall, ratatoskr, mongo, jdbc, desktop)

  lazy val util = Project(id = "util", base = file("util")).
    settings(commonNexusSettings: _*) dependsOn(logging % "test->test")

  lazy val common = Project(id = "common", base = file("common")).
    settings(commonNexusSettings: _*) dependsOn (util, logging % "test->test")

  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).
    settings(commonNexusSettings: _*) dependsOn(logging % "test->test")

  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).
    settings(commonNexusSettings: _*) dependsOn (bytecode % "compile->compile;test->test", util, logging % "test->test")

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
    settings(commonAssemblySettings: _*).dependsOn(standalone, shard)

  lazy val daze = Project(id = "daze", base = file("daze")).
    settings(commonNexusSettings: _*).dependsOn (common, bytecode % "compile->compile;test->test", yggdrasil % "compile->compile;test->test", util, logging % "test->test")

  /// Testing ///

  lazy val muspelheim = Project(id = "muspelheim", base = file("muspelheim")).
    settings(commonNexusSettings: _*) dependsOn (common, quirrel, daze, yggdrasil % "compile->compile;test->test", logging % "test->test")

  lazy val pandora = Project(id = "pandora", base = file("pandora")).
    settings(commonAssemblySettings: _*) dependsOn (quirrel, daze, yggdrasil, ingest, muspelheim % "compile->compile;test->test", logging % "test->test")

  lazy val ragnarok = Project(id = "ragnarok", base = file("ragnarok")).
    settings(commonAssemblySettings: _*).dependsOn(quirrel, daze, yggdrasil, ingest, muspelheim % "compile->compile;test->test", logging % "test->test")

  lazy val gjallerhorn = Project(id = "gjallerhorn", base = file("gjallerhorn")).
    settings(commonAssemblySettings: _*).dependsOn(quirrel, daze, yggdrasil % "compile->test", ingest, muspelheim % "compile->compile;test->test", logging % "test->test")

  lazy val performance = Project(id = "performance", base = file("performance")).
    settings(commonNexusSettings: _*).dependsOn(ingest, common % "compile->compile;test->test", quirrel, daze, yggdrasil, shard, logging % "test->test")

  lazy val jprofiler = Project(id = "jprofiler", base = file("jprofiler")).
    settings(jprofilerSettings ++ commonNexusSettings ++ Seq(fullRunInputTask(profileTask, Test, "com.precog.jprofiler.Run")): _*).dependsOn(ragnarok, logging % "test->test")

  /// Services ///

  lazy val auth = Project(id = "auth", base = file("auth")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", logging % "test->test")

  lazy val accounts     = Project(id = "accounts", base = file("accounts")).
    settings(commonAssemblySettings: _*) dependsOn (common % "compile->compile;test->test", auth, logging % "test->test")

  lazy val ingest = Project(id = "ingest", base = file("ingest")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", yggdrasil, logging % "test->test")

  lazy val heimdall = Project(id = "heimdall", base = file("heimdall")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", util, logging % "test->test")

  lazy val shard = Project(id = "shard", base = file("shard")).
    settings(commonAssemblySettings: _*).dependsOn(common % "compile->compile;test->test", muspelheim, pandora % "test->test")

  /// Tooling ///

  lazy val ratatoskr = Project(id = "ratatoskr", base = file("ratatoskr")).
    settings(commonAssemblySettings: _*).dependsOn(daze, ingest, auth, accounts)
}
