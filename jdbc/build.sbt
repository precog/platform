name := "jdbc"

libraryDependencies ++= Seq(
  "org.eclipse.jetty" % "jetty-server" % "8.1.7.v20120910",
  "com.h2database"    % "h2"           % "1.3.170" % "test",
  "postgresql"        % "postgresql"   % "9.1-901.jdbc4"
)

parallelExecution in Test := true

ivyXML := 
<dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="3.0.0.v201112011016">
<artifact name="javax.servlet" type="orbit" ext="jar"/>
</dependency>

mainClass := Some("com.precog.shard.jdbc.JDBCShardServer")
