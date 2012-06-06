name := "shard"

version := "1.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
      "org.apache"                %% "kafka-core"         % "0.7.5",
      "joda-time"                 % "joda-time"           % "1.6.2",
      "ch.qos.logback"            % "logback-classic"     % "1.0.0"
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

mainClass := Some("com.precog.shard.KafkaShardServer")
