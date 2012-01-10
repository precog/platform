name := "ingest"

version := "1.2.1-SNAPSHOT"

organization := "com.querio"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
      "se.scalablesolutions.akka" % "akka-actor"          % "1.2",
      "se.scalablesolutions.akka" % "akka-typed-actor"    % "1.2",
      "org.apache"                %% "kafka-core"         % "0.7.5",
      "joda-time"                 % "joda-time"           % "1.6.2",
      "org.scalaz"                %% "scalaz-core"        % "6.0.2",
      "ch.qos.logback"            % "logback-classic"     % "1.0.0",
      "org.scala-tools.testing"   %% "scalacheck"         % "1.9",
      "org.specs2"                %% "specs2"             % "1.7-SNAPSHOT"  % "test"
)

resolvers ++= Seq(
  "Scala-Tools Releases" at       "http://scala-tools.org/repo-releases/",
  "Scala-Tools Snapshots" at      "http://scala-tools.org/repo-snapshots/",
  "Akka Repository" at            "http://akka.io/repository/",
  "Nexus Scala Tools" at          "http://nexus.scala-tools.org/content/repositories/releases",
  "Maven Repo 1" at               "http://repo1.maven.org/maven2/"
)

ivyXML :=
  <dependencies>
    <dependency org="org.apache" name="kafka-core_2.9.1" rev="0.7.5">
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
    </dependency>
  </dependencies>

mainClass := Some("com.querio.ingest.service.EchoServer")
