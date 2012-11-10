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
package com.precog.ingest.util

import blueeyes.json._

sealed trait CsvType {
  def apply(s: String): JValue
}

case object CsvType {
  def lub(a: CsvType, b: CsvType): CsvType = (a, b) match {
    case (CsvNothing, b) => b
    case (a, CsvNothing) => a
    case (CsvNum, CsvNum) => CsvNum
    case (_, _) => CsvString
  }

  val Number = """\s*(-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][-+]?[0-9]+)?)\s*""".r
  val Whitespace = """(\s*)""".r

  def infer(x: String): CsvType = x match {
    case Whitespace(_) => CsvNothing
    case Number(x) => CsvNum
    case _ => CsvString
  }

  def inferTypes(rows: Iterator[Array[String]]): Array[CsvType] = {
    val types: Array[CsvType] = rows.next() map (_ => CsvNothing)
    rows foreach { row =>
      var len = row.length min types.length
      var i = 0
      while (i < len) {
        if (types(i) != CsvString) {
          types(i) = CsvType.lub(types(i), CsvType.infer(row(i)))
        }
        i += 1
      }
    }
    types
  }
}

case object CsvString extends CsvType {
  def apply(s: String) = s.trim match {
    case "" => JNull
    case s => JString(s)
  }
}
// case object CsvDate extends CsvType // For when we support DateColumn.
case object CsvNum extends CsvType {
  def apply(s: String) = s match {
    case CsvType.Number(n) => JNum(BigDecimal(n))
    case CsvType.Whitespace(_) => JNull
    case _ => sys.error("Cannot parse CSV number: " + s)
  }
}

case object CsvNothing extends CsvType {
  def apply(s: String) = JNull
}

