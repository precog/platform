name := "mongo"

parallelExecution in Test := false

mainClass := Some("com.precog.shard.mongo.MongoShardServer")
