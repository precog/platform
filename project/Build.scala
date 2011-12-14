import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object PlatformBuild extends Build {
  val condeps = com.samskivert.condep.Depends(
    ("scalaz", "iteratee", "org.scalaz"                  %% "scalaz-iteratee"        % "7.0-SNAPSHOT")
  )

  val commonSettings = Seq(
    resolvers ++= Seq("ReportGrid repo" at            "http://devci01.reportgrid.com:8081/content/repositories/releases",
                      "ReportGrid snapshot repo" at   "http://devci01.reportgrid.com:8081/content/repositories/snapshots"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )
  // Use RG Nexus instance
  lazy val platform = Project(id = "platform", base = file(".")).aggregate(quirrel, storage, bytecode)
  
  lazy val bytecode = Project(id = "bytecode", base = file("bytecode")).settings(commonSettings :_*)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")).dependsOn(bytecode).settings(commonSettings :_*)
  
  lazy val storage = condeps.addDeps(Project(
    "storage", file("storage/leveldbstore"), 
    settings = Defaults.defaultSettings ++ commonSettings ++ Seq(

      version := "0.0.1-SNAPSHOT",

      organization := "com.reportgrid",

      scalaVersion := "2.9.1",

      scalacOptions ++= Seq("-deprecation", "-unchecked"),

      libraryDependencies ++= Seq(
        "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.1-SNAPSHOT",
        "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.1-SNAPSHOT",
        "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
        "org.specs2"                  %% "specs2"             % "1.7-SNAPSHOT"   % "test",
        "org.scala-tools.testing"     %% "scalacheck"         % "1.9",
        "se.scalablesolutions.akka"   %  "akka-actor"         % "1.2",
        "ch.qos.logback"              %  "logback-classic"    % "1.0.0"
      ),

      resolvers ++= Seq(
        "Local Maven Repository" at     "file://"+Path.userHome.absolutePath+"/.m2/repository",
        "Scala-Tools Releases" at       "http://scala-tools.org/repo-releases/",
        "Scala-Tools Snapshots" at      "http://scala-tools.org/repo-snapshots/",
        "Akka Repository" at            "http://akka.io/repository/",
        "Nexus Scala Tools" at          "http://nexus.scala-tools.org/content/repositories/releases",
        "Maven Repo 1" at               "http://repo1.maven.org/maven2/"
      )
    ) ++ seq(assemblySettings: _*)
  ))
}

