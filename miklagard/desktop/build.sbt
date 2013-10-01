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

import sbtassembly.Plugin._
import AssemblyKeys._

// Workaround for dependency breakage due to zkclient in kafka-core
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", "zookeeper", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jute", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  }
}
