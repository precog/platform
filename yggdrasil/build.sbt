import AssemblyKeys._

name := "yggdrasil"

version := "0.0.1-SNAPSHOT"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none", "-Xexperimental", "-optimise")//, "-Ydebug")
//scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none", "-optimise")

fork := true

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

parallelExecution in test := false

libraryDependencies ++= Seq(
  "commons-primitives"          %  "commons-primitives" % "1.0",
  "net.sf.opencsv"              % "opencsv"             % "2.0",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0",
  "com.typesafe.akka"           %  "akka-testkit"       % "2.0" % "test",
  "org.apache"                  %% "kafka-core"         % "0.7.5",
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.2-SNAPSHOT" changing(),
  "com.github.scopt"            %  "scopt_2.9.1"        % "2.0.1",
  "org.fusesource.leveldbjni"   %  "leveldbjni-osx"     % "1.2-SNAPSHOT" changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.2-SNAPSHOT" changing(),
  "org.apfloat"                 %  "apfloat"            % "1.6.3"
)

ivyXML :=
  <dependencies>
    <dependency org="org.apache" name="kafka-core_2.9.2" rev="0.7.5">
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
      <exclude org="jline"/>
    </dependency>
  </dependencies>

seq(assemblySettings: _*)

mainClass := Some("com.precog.yggdrasil.shard.KafkaShardServer")
