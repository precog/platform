name := "daze"

scalacOptions += "-Ydependent-method-types"

libraryDependencies ++= Seq(
  "com.eed3si9n" %  "treehugger_2.9.1" % "0.1.2",
  "com.chuusai" %% "shapeless" % "1.2.2")
  
logBuffered := false       // gives us incremental output from Specs2

parallelExecution in Test := false
