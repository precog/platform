name := "mongo"

parallelExecution in Test := true

mainClass := Some("com.precog.shard.mongo.MongoShardServer")
