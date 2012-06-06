name := "util"

version := "0.1.0"

libraryDependencies ++= Seq(
  "com.reportgrid"          % "blueeyes-json_2.9.1" % "0.6.0-SNAPSHOT" changing(),
  "commons-io"              % "commons-io"          % "2.3",
  "joda-time"               % "joda-time"           % "1.6.2"
)
  
logBuffered := false       // gives us incremental output from Specs2
