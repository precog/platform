import AssemblyKeys._

name := "yggdrasil"

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

//fork in test := true

parallelExecution in test := false

libraryDependencies ++= Seq(
  "commons-primitives"          %  "commons-primitives" % "1.0",
  "net.sf.opencsv"              %  "opencsv"            % "2.0",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.github.scopt"            %  "scopt_2.9.1"        % "2.0.1",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0.5",
  "org.quartz-scheduler"        %  "quartz"             % "2.1.7",
  "com.typesafe.akka"           %  "akka-testkit"       % "2.0.5" % "test"
)
