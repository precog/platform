name := "util"

libraryDependencies ++= Seq(
  "com.reportgrid"          %% "blueeyes-json"       % "0.6.1-SNAPSHOT" changing(),
  "commons-io"              %  "commons-io"          % "2.3",
  "joda-time"               %  "joda-time"           % "1.6.2"
)
  
logBuffered := false       // gives us incremental output from Specs2
