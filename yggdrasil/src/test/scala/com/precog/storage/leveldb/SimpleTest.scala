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
package com.precog.storage
package leveldb

import com.precog.util.Bijection
import com.precog.yggdrasil.leveldb.LevelDBProjection

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

object SimpleTest {
  def main (argv : Array[String]) {
    val c = LevelDBProjection(new File("/tmp/test"), sys.error("todo") /* Some(ProjectionComparator.BigDecimal)*/) ||| {
      errors => for (err <- errors.list) err.printStackTrace
                sys.error("Errors prevented creation of LevelDBProjection")
    }

    c.insert(Vector(12364534l), sys.error("todo") /*new BigDecimal("1.445322").as[Array[Byte]].as[ByteBuffer]*/)
  }
}

// vim: set ts=4 sw=4 et:
