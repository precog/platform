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
package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

import blueeyes.json._

sealed abstract class Segment(val blockid: Long, val cpath: CPath, val ctype: CType, val defined: BitSet)

case class ArraySegment[A](id: Long, cp: CPath, ct: CValueType[A], d: BitSet, values: Array[A])
    extends Segment(id, cp, ct, d)

case class BooleanSegment(id: Long, cp: CPath, d: BitSet, values: BitSet)
    extends Segment(id, cp, CBoolean, d)

case class NullSegment[A](id: Long, cp: CPath, ct: CNullType, d: BitSet, values: Array[A])
    extends Segment(id, cp, ct, d)
