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
package com.reportgrid.storage.leveldb

import comparators.ColumnComparator

import java.io.File

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.{BeforeAfter,Specification}
import org.specs2.specification.Scope
import Bijection._

import com.weiglewilczek.slf4s.Logging

class ColumnSpec extends Specification with ScalaCheck with ThrownMessages with Logging {
  trait columnSetup extends Scope with BeforeAfter {
    val dataDir = File.createTempFile("ColumnSpec", ".db")
    logger.info("Using %s for dbtest".format(dataDir))
    def before = dataDir.delete() // Ugly, but it works
    def after = {
      // Here we need to remove the entire directory and contents
      def delDir (dir : File) {
        dir.listFiles.foreach {
          case d if d.isDirectory => delDir(d)
          case f => f.delete()
        }
        dir.delete()
      }
      delDir(dataDir)
    }
  }

  "Columns" should {
    "Fail to create a new column without a provided comparator" in new columnSetup {
      Column(dataDir).isFailure must_== true
    }

    "Create a new column with a provided comparator" in new columnSetup {
      val c = Column(dataDir, Some(ColumnComparator.Long))
      c.isSuccess must_== true
      c.map(_.close())
    }

    "Open an existing column with a restored comparator" in new columnSetup {
      val initial = Column(dataDir, Some(ColumnComparator.Long))
      initial.isSuccess must_== true
      initial.map(_.close()).flatMap(_ => Column(dataDir)).isSuccess must_== true
    }


//    "Properly persist ID lists" in {
//      check { (v : BigDecimal, ids : List[Long]) =>
//        ids.foreach(c.insert(_, v.underlying))
//        
//        val resultIds = c.getIds(v.underlying)
//
//        resultIds.size must_== ids.size
//      }
//    }
  }
}
