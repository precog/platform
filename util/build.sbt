name := "util"

organization := "com.precog"

version := "0.1.0"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "com.reportgrid"          %% "blueeyes-json"  % "0.6.0-SNAPSHOT" changing(),
  "joda-time"               % "joda-time"       % "1.6.2",
  "org.scalaz"              %% "scalaz-core"    % "7.0-SNAPSHOT"   changing(),
  "org.scalaz"              %% "scalaz-effect"  % "7.0-SNAPSHOT"   changing(),
  "org.scala-tools.testing" %% "scalacheck"     % "1.9" % "test",
  "org.specs2"              %% "specs2"         % "1.8" % "test"
)
  
logBuffered := false       // gives us incremental output from Specs2
