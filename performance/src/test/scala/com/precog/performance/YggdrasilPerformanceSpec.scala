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
package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.common._
import com.precog.shard.yggdrasil._

import akka.dispatch.Await
import akka.util.Duration

import java.io.File

trait YggdrasilPerformanceSpec extends Specification with PerformanceSpec {
  "yggdrasil" should {
    val tmpFile = File.createTempFile("insert_test", "_db")
  
    step {    
      tmpFile.delete
      tmpFile.mkdirs
    }

    "insert 1M elements in 6s".performBatch(1000000, 8000) { i =>
      todo
    }

    "read 1M elements in 1s".performBatch(1000000, 1500) { i =>
      todo
    }
   
    // need cleanup here...
  }
}

