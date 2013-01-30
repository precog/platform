name := "mongo"

libraryDependencies ++= Seq(
  "org.eclipse.jetty" % "jetty-server"      % "8.1.3.v20120416"
)

parallelExecution in Test := false

ivyXML := 
  <dependencies>
    <dependency org="org.eclipse.jetty" name="jetty-server" rev="8.1.3.v20120416">
      <exclude org="org.eclipse.jetty.orbit"/>
    </dependency>
  </dependencies>

mainClass := Some("com.precog.shard.mongo.MongoShardServer")
