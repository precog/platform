name := "common"

version := "1.2.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "joda-time"                 % "joda-time"           % "1.6.2",
  "org.streum"                %% "configrity"         % "0.9.0",
  "com.reportgrid"            %% "blueeyes-json"      % "0.6.0-SNAPSHOT" changing(),
  "com.reportgrid"            %% "blueeyes-core"      % "0.6.0-SNAPSHOT" changing(),
  "com.reportgrid"            %% "blueeyes-mongo"     % "0.6.0-SNAPSHOT" changing(),
  "org.scala-tools.testing"   %% "scalacheck"         % "1.9",
  "org.specs2"                %% "specs2"             % "1.7"  % "test"
)
