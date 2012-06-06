name := "muspelheim"

version := "0.0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.reflections" % "reflections" % "0.9.5" % "test"
)
  
logBuffered := false       // gives us incremental output from Specs2
