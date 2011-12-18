import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object PlatformBuild extends Build {
  val scalaz = com.samskivert.condep.Depends(
    ("scalaz", "iteratee", "org.scalaz"                  %% "scalaz-iteratee"        % "7.0-SNAPSHOT")
  )

  val blueeyesDeps = com.samskivert.condep.Depends( 
    ("blueeyes",         null, "com.reportgrid"                  %% "blueeyes"         % "0.5.0-SNAPSHOT")
  )

  val ingestDeps = com.samskivert.condep.Depends(
    ("client-libraries", null, "com.reportgrid"                  %% "client-libraries" % "0.3.1")
  )
  
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
  lazy val storage = scalaz.addDeps(Project(id = "storage", base = file("storage")).settings(nexusSettings : _*))
  
  lazy val daze = Project(id = "daze", base = file("daze")).settings(nexusSettings : _*) dependsOn bytecode // (bytecode, storage)
  
  lazy val common = blueeyesDeps.addDeps(Project(id = "common", base = file("common")).settings(nexusSettings: _*))

  lazy val ingest = ((blueeyesDeps.addDeps _) andThen (ingestDeps.addDeps _))(Project(id = "ingest", base = file("ingest")).settings(nexusSettings: _*) dependsOn(common))
}

