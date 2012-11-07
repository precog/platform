name := "util"

libraryDependencies ++= Seq(
  "com.reportgrid"          %% "blueeyes-json"       % "1.0.0-SNAPSHOT" changing(),
  "commons-io"              %  "commons-io"          % "2.3",
  "com.google.guava"        %  "guava"               % "12.0",
  "joda-time"               %  "joda-time"           % "1.6.2"
)
  
logBuffered := false       // gives us incremental output from Specs2
