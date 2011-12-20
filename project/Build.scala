import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object PlatformBuild extends Build {
  val scalaz = com.samskivert.condep.Depends(
    ("scalaz", "core", "org.scalaz"                  %% "scalaz-core"        % "7.0-SNAPSHOT"),
    ("scalaz", "effect", "org.scalaz"                  %% "scalaz-effect"        % "7.0-SNAPSHOT"),
    ("scalaz", "iteratee", "org.scalaz"                  %% "scalaz-iteratee"        % "7.0-SNAPSHOT")
  )

  val blueeyesDeps = com.samskivert.condep.Depends( 
    ("blueeyes",         null, "com.reportgrid"                  %% "blueeyes"         % "0.5.0-SNAPSHOT")
  )

  val clientLibDeps = com.samskivert.condep.Depends(
    ("client-libraries", null, "com.reportgrid"                  %% "scala-client" % "0.3.1")
  )

  val nexusSettings : Seq[Project.Setting[_]] = Seq(
    resolvers ++= Seq("ReportGrid repo"          at   "http://nexus.reportgrid.com/content/repositories/releases",
                      "ReportGrid repo (public)" at   "http://nexus.reportgrid.com/content/repositories/public-releases",
                      "ReportGrid snapshot repo"          at   "http://nexus.reportgrid.com/content/repositories/snapshots",
                      "ReportGrid snapshot repo (public)" at   "http://nexus.reportgrid.com/content/repositories/public-snapshots"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )

  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel, storage, bytecode, daze, ingest)
  
  lazy val blueeyes = RootProject(uri("../blueeyes"))
  lazy val clientLibraries = RootProject(uri("../client-libraries/scala"))
  
  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(nexusSettings : _*)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).settings(nexusSettings : _*) dependsOn bytecode
  
  lazy val daze = Project(id = "daze", base = file("daze")).settings(nexusSettings : _*) dependsOn bytecode // (bytecode, storage)
  
  val commonSettings = nexusSettings ++ Seq(libraryDependencies ++= blueeyesDeps.libDeps)
  lazy val common = blueeyesDeps.addDeps(Project(id = "common", base = file("common")).settings(commonSettings: _*))

  val storageSettings = nexusSettings ++ Seq(libraryDependencies ++= scalaz.libDeps)
  lazy val storage = scalaz.addDeps(Project(id = "storage", base = file("storage")).settings(storageSettings : _*).dependsOn(common))
  
  val ingestSettings = sbtassembly.Plugin.assemblySettings ++ nexusSettings ++ Seq(libraryDependencies ++= clientLibDeps.libDeps)
  lazy val ingest = clientLibDeps.addDeps(Project(id = "ingest", base = file("ingest")).settings(ingestSettings: _*).dependsOn(common))
}

