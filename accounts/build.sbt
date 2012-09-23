name := "accounts"

libraryDependencies ++= Seq(
      "joda-time"                 % "joda-time"           % "1.6.2",
      "org.streum"                % "configrity_2.9.1"     % "0.9.0",
      "org.scalaz"                %% "scalaz-core"        % "7.0-SNAPSHOT" changing(),
      "ch.qos.logback"            % "logback-classic"     % "1.0.0",
      "com.reportgrid"           %% "blueeyes-json"       % "0.6.1-SNAPSHOT" changing(),
      "com.reportgrid"            %% "blueeyes-core"       % "0.6.1-SNAPSHOT" changing(),
      "com.reportgrid"            %% "blueeyes-mongo"      % "0.6.1-SNAPSHOT" changing()
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
  
mainClass := Some("com.precog.accounts.MongoAccountServer")



