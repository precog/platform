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
package reportgrid.storage.leveldb

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.Specification
import Bijection._

class ColumnSpec extends Specification with ScalaCheck with ThrownMessages {
//  "Columns" should {
//    val c = new Column("test", "/tmp")
//
//    "Properly persist ID lists" in {
//      check { (v : BigDecimal, ids : List[Long]) =>
//        ids.foreach(c.insert(_, v.underlying))
//        
//        val resultIds = c.getIds(v.underlying)
//
//        resultIds.size must_== ids.size
//      }
//    }
//  }
}
