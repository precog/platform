import AssemblyKeys._

name := "yggdrasil"

version := "0.0.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

fork := true

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

libraryDependencies ++= Seq(
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0",
  "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
  "org.apache"                  %% "kafka-core"         % "0.7.5",
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.2-SNAPSHOT"                changing(),
  "org.scalaz"                  %% "scalaz-core"        % "7.0-SNAPSHOT"                changing(),
  "org.scalaz"                  %% "scalaz-effect"      % "7.0-SNAPSHOT"                changing(),
  "org.scalaz"                  %% "scalaz-iteratee"    % "7.0-SNAPSHOT"                changing(),
  "org.scala-tools.testing"     %% "scalacheck"         % "1.9"            % "test",
  "org.specs2"                  %% "specs2"             % "1.8"   % "test",
  "org.fusesource.leveldbjni"   %  "leveldbjni-osx"     % "1.2-SNAPSHOT"   changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.2-SNAPSHOT"   changing() 
)

ivyXML :=
  <dependencies>
    <dependency org="org.apache" name="kafka-core_2.9.1" rev="0.7.5">
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
      <exclude org="jline"/>
    </dependency>
  </dependencies>

seq(assemblySettings: _*)

mainClass := Some("com.precog.yggdrasil.shard.KafkaShardServer")
