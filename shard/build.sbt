name := "shard"

libraryDependencies ++= Seq(
  "ch.qos.logback"            % "logback-classic"     % "1.0.0",
  "org.quartz-scheduler"      %  "quartz"             % "2.1.7"
)

mainClass := Some("com.precog.shard.nihdb.NIHDBShardServer")
