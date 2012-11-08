name := "auth"

libraryDependencies ++= Seq(
      "joda-time"                 % "joda-time"           % "1.6.2",
      "org.scalaz"                %% "scalaz-core"        % "7.0-SNAPSHOT" changing(),
      "ch.qos.logback"            % "logback-classic"     % "1.0.0",
      "com.chuusai"               %% "shapeless"          % "1.2.3-SNAPSHOT" changing()
)

mainClass := Some("com.precog.auth.MongoAPIKeyServer")
