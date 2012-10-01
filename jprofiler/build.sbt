/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
  if (!data.mkdirs())
    throw new Exception("Failed to create %s" format path)
  try {
    s.log.info("Extracting data into %s" format path)
    val args = Seq(path, System.getProperty("java.class.path"))
    Process("./regen-jdbm-data.sh", args).!!
  } catch {
    case t: Throwable => s.log.error("Failed to extract to %s" format path)
  }
  d.getCanonicalPath
}
