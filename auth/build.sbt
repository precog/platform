name := "auth"

version := "1.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
      "joda-time"                 % "joda-time"           % "1.6.2",
      "org.scalaz"                %% "scalaz-core"        % "7.0-SNAPSHOT" changing(),
      "ch.qos.logback"            % "logback-classic"     % "1.0.0"
)

mainClass := Some("com.precog.auth.MongoTokenServer")
