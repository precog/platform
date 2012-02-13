import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object PlatformBuild extends Build {
  val nexusSettings : Seq[Project.Setting[_]] = Seq(
    resolvers ++= Seq("ReportGrid repo"                   at "http://nexus.reportgrid.com/content/repositories/releases",
                      "ReportGrid repo (public)"          at "http://nexus.reportgrid.com/content/repositories/public-releases",
                      "ReportGrid snapshot repo"          at "http://nexus.reportgrid.com/content/repositories/snapshots",
                      "ReportGrid snapshot repo (public)" at "http://nexus.reportgrid.com/content/repositories/public-snapshots",
                      "Typesafe Repository"               at "http://repo.typesafe.com/typesafe/releases/",
                      "Scala Tools"                       at "http://scala-tools.org/repo-releases/",
                      "Scala-Tools Snapshots"             at "http://scala-tools.org/repo-snapshots/",
                      "Maven Repo 1"                      at "http://repo1.maven.org/maven2/",
                      "Guiceyfruit"                       at "http://guiceyfruit.googlecode.com/svn/repo/releases/"),

    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials"),

    publishTo <<= (version) { version: String =>
      val nexus = "http://nexus.reportgrid.com/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/") 
      else                                   Some("releases"  at nexus+"releases/")
    }
  )

  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel, yggdrasil, bytecode, daze, ingest, pandora, util, common)
  
  lazy val common   = Project(id = "common", base = file("common")).settings(nexusSettings : _*)
  lazy val util     = Project(id = "util", base = file("util")).settings(nexusSettings: _*)

  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(nexusSettings: _*)
  lazy val quirrel  = Project(id = "quirrel", base = file("quirrel")).settings(nexusSettings: _*) dependsOn (bytecode, util)
  
  lazy val daze     = Project(id = "daze", base = file("daze")).settings(nexusSettings : _*) dependsOn (common, bytecode, yggdrasil % "compile->compile;test->test", util)
  
  val pandoraSettings = sbtassembly.Plugin.assemblySettings ++ nexusSettings
  lazy val pandora  = Project(id = "pandora", base = file("pandora")).settings(pandoraSettings : _*) dependsOn (quirrel, daze, yggdrasil, ingest)
  
  lazy val yggdrasil  = Project(id = "yggdrasil", base = file("yggdrasil")).settings(nexusSettings : _*).dependsOn(common, util)
  
  val ingestSettings = sbtassembly.Plugin.assemblySettings ++ nexusSettings
  lazy val ingest   = Project(id = "ingest", base = file("ingest")).settings(ingestSettings: _*).dependsOn(common, quirrel, daze, yggdrasil)

  val dist = TaskKey[Unit]("dist", "builds dist")
  val dataDir = SettingKey[String]("data-dir", "The temporary directory into which to extract the test data")
  val extractData = TaskKey[String]("extract-data", "Extracts the LevelDB data files used by the tests and the REPL")
  val mainTest = SettingKey[String]("main-test", "The primary test class for the project (just used for pandora)")
}

