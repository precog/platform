name := "daze"

version := "0.0.1-SNAPSHOT"

organization := "com.quirio"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.scalaz"                  %% "scalaz-core"        % "7.0-SNAPSHOT",
  "org.specs2"                  %% "specs2"             % "1.8-SNAPSHOT"   % "test",
  "org.scala-tools.testing"     %% "scalacheck"         % "1.9",
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
