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
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.1-SNAPSHOT",
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.1-SNAPSHOT",
  "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
  "org.specs2"                  %% "specs2"             % "1.7-SNAPSHOT"   % "test",
  "org.scala-tools.testing"     %% "scalacheck"         % "1.9",
  "se.scalablesolutions.akka"   %  "akka-actor"         % "1.2",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0"
)

resolvers ++= Seq(
  "Local Maven Repository" at     "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Scala-Tools Releases" at       "http://scala-tools.org/repo-releases/",
  "Scala-Tools Snapshots" at      "http://scala-tools.org/repo-snapshots/",
  "Akka Repository" at            "http://akka.io/repository/",
  "Nexus Scala Tools" at          "http://nexus.scala-tools.org/content/repositories/releases",
  "Maven Repo 1" at               "http://repo1.maven.org/maven2/"
)

seq(assemblySettings: _*)
