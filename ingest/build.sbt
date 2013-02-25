name := "ingest"

parallelExecution in test := false

libraryDependencies ++= Seq(
      "ch.qos.logback"            %  "logback-classic"    % "1.0.0"
)

mainClass := Some("com.precog.ingest.kafka.KafkaEventServer")
