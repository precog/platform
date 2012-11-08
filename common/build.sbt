name := "common"

scalacOptions += "-Ydependent-method-types"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "1.6.2",
  "org.streum" % "configrity_2.9.1" % "0.9.0",
  "org.apache" %% "kafka-core" % "0.7.5",
  "com.google.guava" %  "guava" % "12.0",
  "com.chuusai" %% "shapeless" % "1.2.3-SNAPSHOT" changing()
  //// 1.6.1 due to slf4s dep in master project
  //"org.slf4j" %  "slf4j-simple" % "1.6.1" % "test",
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

parallelExecution in test := false
