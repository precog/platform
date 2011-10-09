scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.specs2" %% "specs2" % "1.5" % "test" withSources)
