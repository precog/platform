// 
//  ____    ____    _____    ____    ___     ____ 
// |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
// | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
// |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
// |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
// 
// This program is free software: you can redistribute it and/or modify it under the terms of the 
// GNU Affero General Public License as published by the Free Software Foundation, either version 
// 3 of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
// without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
// the GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License along with this 
// program. If not, see <http://www.gnu.org/licenses/>.
// 
// 

import AssemblyKeys._

name := "yggdrasil"

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

//fork in test := true

parallelExecution in test := false

libraryDependencies ++= Seq(
  "commons-primitives"          %  "commons-primitives" % "1.0",
  "net.sf.opencsv"              %  "opencsv"            % "2.0",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.github.scopt"            %  "scopt_2.9.1"        % "2.0.1",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0.5",
  "org.quartz-scheduler"        %  "quartz"             % "2.1.7",
  "com.typesafe.akka"           %  "akka-testkit"       % "2.0.5" % "test"
)
