name := "muspelheim"

libraryDependencies ++= Seq(
  "org.reflections" % "reflections" % "0.9.5" % "test",
  "com.typesafe.akka"           %  "akka-testkit"       % "2.0" % "test"
)
  
logBuffered := false       // gives us incremental output from Specs2
