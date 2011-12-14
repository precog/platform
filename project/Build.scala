import sbt._
import Keys._

object PlatformBuild extends Build {
  val commonSettings = Seq(
    resolvers ++= Seq("ReportGrid repo" at            "http://devci01.reportgrid.com:8081/content/repositories/releases",
                      "ReportGrid snapshot repo" at   "http://devci01.reportgrid.com:8081/content/repositories/snapshots"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )
  // Use RG Nexus instance
  lazy val platform = Project(id = "platform", base = file(".")).aggregate(quirrel, storage, bytecode)
  
  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(commonSettings :_*)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).dependsOn(bytecode).settings(commonSettings :_*)
  lazy val storage = Project(id = "storage", base = file("storage/leveldbstore")).settings(commonSettings :_*)
}

