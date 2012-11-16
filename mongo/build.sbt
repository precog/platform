name := "mongo"

libraryDependencies ++= Seq(
  "org.eclipse.jetty" % "jetty-server"      % "8.1.7.v20120910"
)

ivyXML := 
<dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="3.0.0.v201112011016">
<artifact name="javax.servlet" type="orbit" ext="jar"/>
</dependency>

mainClass := Some("com.precog.shard.mongo.MongoShardServer")