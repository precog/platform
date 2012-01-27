import AssemblyKeys._

name := "yggdrasil"

version := "0.0.1-SNAPSHOT"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

fork := true

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

libraryDependencies ++= Seq(
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0-M1",
  "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
  "org.apache"                  %% "kafka-core"         % "0.7.5",
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.1-SNAPSHOT"                changing(),
  "org.scalaz"                  %% "scalaz-core"        % "7.0-SNAPSHOT"                changing(),
  "org.scalaz"                  %% "scalaz-effect"      % "7.0-SNAPSHOT"                changing(),
  "org.scalaz"                  %% "scalaz-iteratee"    % "7.0-SNAPSHOT"                changing(),
  "org.scala-tools.testing"     %% "scalacheck"         % "1.9"            % "test",
  "org.specs2"                  %% "specs2"             % "1.7"   % "test",
  // shady I know, but that's how I roll...
  if(System.getProperty("os.name") == "Max OS X") {
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.1-SNAPSHOT"   % "optional" changing() } else {
  "org.fusesource.leveldbjni"   %  "leveldbjni-osx"     % "1.1-SNAPSHOT"   % "optional" changing() } 
)

resolvers ++= Seq(
  "Local Maven Repository" at     "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

ivyXML :=
  <dependencies>
    <dependency org="org.apache" name="kafka-core_2.9.1" rev="0.7.5">
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
    </dependency>
  </dependencies>

seq(assemblySettings: _*)
