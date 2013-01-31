name := "jdbc"

libraryDependencies ++= Seq(
  "com.h2database"    % "h2"           % "1.3.170" % "test",
  "postgresql"        % "postgresql"   % "9.1-901.jdbc4"
)

parallelExecution in Test := true

mainClass := Some("com.precog.shard.jdbc.JDBCShardServer")
