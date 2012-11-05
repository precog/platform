name := "mongo"

libraryDependencies ++= Seq(
  "com.reportgrid"          %% "blueeyes-mongo"       % "1.0.0-SNAPSHOT" changing(),
  "org.mongodb" % "mongo-java-driver"   % "2.9.3"
)

mainClass := Some("com.precog.shard.mongo.MongoShardServer")
