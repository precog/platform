name := "auth"

libraryDependencies ++= Seq(
      "ch.qos.logback"            % "logback-classic"     % "1.0.0",
      "com.chuusai"               %% "shapeless"          % "1.2.3-SNAPSHOT" changing()
)

mainClass := Some("com.precog.auth.MongoAPIKeyServer")
