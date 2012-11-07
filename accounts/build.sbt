name := "accounts"

libraryDependencies ++= Seq(
  "org.streum" % "configrity_2.9.1" % "0.9.0",
  "ch.qos.logback" % "logback-classic" % "1.0.0"
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



