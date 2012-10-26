name := "common"

scalacOptions += "-Ydependent-method-types"

libraryDependencies ++= Seq(
  "joda-time"                 % "joda-time"            % "1.6.2",
  "org.streum"                % "configrity_2.9.1"     % "0.9.0",
  "org.apache"                %% "kafka-core"          % "0.7.5",
  "com.reportgrid"            %% "blueeyes-core"       % "1.0.0-SNAPSHOT" changing(),
  "com.reportgrid"            %% "blueeyes-mongo"      % "1.0.0-SNAPSHOT" changing(),
  "com.chuusai"               %% "shapeless"           % "1.2.3-SNAPSHOT" changing())

ivyXML :=
  <dependencies>
    <dependency org="org.apache" name="kafka-core_2.9.1" rev="0.7.5">
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
      <exclude org="jline"/>
    </dependency>
  </dependencies>
