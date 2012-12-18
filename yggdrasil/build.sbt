import AssemblyKeys._

name := "yggdrasil"

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

parallelExecution in test := false

libraryDependencies ++= Seq(
  "commons-primitives"          %  "commons-primitives" % "1.0",
  "net.sf.opencsv"              %  "opencsv"             % "2.0",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0.2",
  "com.typesafe.akka"           %  "akka-testkit"       % "2.0.2" % "test",
  "org.apfloat"                 %  "apfloat"            % "1.6.3",
  "org.spire-math"              %% "spire"              % "0.2.0-M2"
)
