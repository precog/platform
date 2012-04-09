name := "muspelheim"

version := "0.0.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

resolvers ++= Seq(
  "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots",
  "sonatype" at "https://oss.sonatype.org/content/groups/public")

scalacOptions ++= Seq("-deprecation", "-g:none")

libraryDependencies ++= Seq(
  "org.reflections" % "reflections" % "0.9.5" % "test",
  "org.specs2" %% "specs2" % "1.8" % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"
)
  
logBuffered := false       // gives us incremental output from Specs2
