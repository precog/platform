name := "auth"

version := "1.0.0-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

libraryDependencies ++= Seq(
      "joda-time"                 % "joda-time"           % "1.6.2",
      "org.scalaz"                %% "scalaz-core"        % "6.0.2",
      "ch.qos.logback"            % "logback-classic"     % "1.0.0",
      "org.scala-tools.testing"   %% "scalacheck"         % "1.9",
      "org.specs2"                %% "specs2"             % "1.8"  % "test"
)

mainClass := Some("com.precog.auth.MongoTokenServer")
