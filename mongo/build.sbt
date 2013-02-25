name := "mongo"

parallelExecution in Test := false

fork in ScctTest := true

javaOptions in ScctTest := Seq("-Xmx2G", "-XX:MaxPermSize=512m") // required if using fork in Test

mainClass := Some("com.precog.shard.mongo.MongoShardServer")
