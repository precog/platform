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
 
dataDir := {
  new java.io.File("jprofiler/jprofiler.db").getCanonicalPath
}

extractData <<= (dataDir, streams) map { (dir, s) =>
  val d = new File(dir)
  val data = new File(d, "data")
  val path = data.getCanonicalPath
  if (d.exists) {
    s.log.info("Deleting old data from %s" format path)
    sbt.IO.delete(d)
  }
  def performExtract = {
    s.log.info("Extracting data into %s" format path)
    if (Process("./regen-jdbm-data.sh", Seq(path)).! != 0) {
      error("Failed to extract to %s" format path)
    } else {
      s.log.info("Extraction complete.")
      d.getCanonicalPath
    }
  }  
  if (!data.isDirectory()) {
    if (!data.mkdirs()) {
      error("Failed to create %s")
    } else {
      performExtract
    }
  } else {
    if (data.listFiles.length > 0) {
      s.log.info("Using data in %s" format path)
      d.getCanonicalPath  
    } else {
      performExtract
    }
  }
}
