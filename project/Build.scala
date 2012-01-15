import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object PlatformBuild extends Build {
  val clientLibDeps = com.samskivert.condep.Depends(
    ("client-libraries", null, "com.reportgrid"                  %% "scala-client" % "0.3.1")
  )

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

    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )

  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel, storage, bytecode, daze, ingest)
  
  lazy val common   = Project(id = "common", base = file("common")).settings(nexusSettings : _*)
  lazy val util     = Project(id = "util", base = file("util")).settings(nexusSettings: _*)

  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(nexusSettings: _*) dependsOn util
  lazy val quirrel  = Project(id = "quirrel", base = file("quirrel")).settings(nexusSettings: _*) dependsOn (bytecode, util)
  
  lazy val daze     = Project(id = "daze", base = file("daze")).settings(nexusSettings : _*) dependsOn bytecode // (bytecode, storage)
  
  lazy val storage  = Project(id = "storage", base = file("storage")).settings(nexusSettings : _*).dependsOn(common)
  
  val ingestSettings = sbtassembly.Plugin.assemblySettings ++ nexusSettings ++ Seq(libraryDependencies ++= clientLibDeps.libDeps)
  lazy val ingest   = Project(id = "ingest", base = file("ingest")).settings(ingestSettings: _*).dependsOn(common)
}

