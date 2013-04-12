name := "daze"

scalacOptions += "-Ydependent-method-types"

libraryDependencies ++= Seq(
  "com.eed3si9n"       % "treehugger_2.9.1"   % "0.1.2",
  "gov.nist.math"      % "jama"               % "1.0.2",
  "org.apache.commons" % "commons-math3"      % "3.1.1" 
)
  
logBuffered := false       // gives us incremental output from Specs2

parallelExecution in Test := false
