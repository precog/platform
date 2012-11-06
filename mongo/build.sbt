name := "mongo"

libraryDependencies ++= Seq(
  "com.reportgrid"    %% "blueeyes-mongo"   % "1.0.0-SNAPSHOT" changing(),
  "org.mongodb"       % "mongo-java-driver" % "2.9.3",
  "org.eclipse.jetty" % "jetty-server"      % "8.1.7.v20120910"
)

ivyXML := 
<dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="3.0.0.v201112011016">
<artifact name="javax.servlet" type="orbit" ext="jar"/>
</dependency>

mainClass := Some("com.precog.shard.mongo.MongoShardServer")
