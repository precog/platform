scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "jline" % "jline" % "0.9.9",
  "edu.uwm.cs" %% "gll-combinators" % "1.5-SNAPSHOT",
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.specs2" %% "specs2" % "1.5" % "test" withSources)
