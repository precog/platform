name := "common"

scalacOptions += "-Ydependent-method-types"

libraryDependencies ++= Seq(
  "ch.qos.logback" %  "logback-classic"    % "1.0.0",
  "org.streum"     %% "configrity-core"    % "0.10.2",
  "org.apache"     %% "kafka-core" % "0.7.5",
  "com.chuusai"    %% "shapeless" % "1.2.3"
)

ivyXML :=
  <dependencies>
    <dependency org="org.apache" name="kafka-core_2.9.2" rev="0.7.5">
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
      <exclude org="jline"/>
      <exclude org="org.apache.hadoop"/>
      <exclude org="org.apache.avro"/>
    </dependency>
  </dependencies>

parallelExecution in test := false
