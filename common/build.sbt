name := "common"

version := "1.2.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

libraryDependencies ++= Seq(
  "joda-time"                 % "joda-time"           % "1.6.2",
  "org.streum"                %% "configrity"         % "0.9.0",
  "org.apache"                %% "kafka-core"         % "0.7.5",
  "com.reportgrid"            %% "blueeyes-json"      % "0.6.0-SNAPSHOT" changing(),
  "com.reportgrid"            %% "blueeyes-core"      % "0.6.0-SNAPSHOT" changing(),
  "com.reportgrid"            %% "blueeyes-mongo"     % "0.6.0-SNAPSHOT" changing(),
  "org.scala-tools.testing"   %% "scalacheck"         % "1.9",
  "org.specs2"                %% "specs2"             % "1.8"  % "test"
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
