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
package com.precog.yggdrasil

import blueeyes.json.JPath
import com.precog.common.Path

sealed trait ColumnRef {
  type CA
}

case class IColumnRef(idx: Int) extends ColumnRef {
  type CA = Long
}

case class VColumnRef[@specialized(Boolean, Long, Double) A](id: VColumnId, ctype: CType { type CA = A }) extends ColumnRef {
  type CA = ctype.CA
}

object VColumnRef {
  /*
  @inline def apply[A](id: VColumnId, ctype: CType { type CA = A }): VColumnRef { type CA = A } = {
    new VColumnRef(id, ctype).asInstanceOf[VColumnRef { type CA = A }]
  }

  def unapply(ref: VColumnRef): Option[(VColumnId, CType)] = {
    Some((ref.id, ref.ctype))
  }

  @inline def cast[A](ref: VColumnRef) = ref.asInstanceOf[VColumnRef { type CA = A }]
  */
}

sealed trait VColumnId 
case class NamedColumnId(path: Path, selector: JPath) extends VColumnId
case class DynColumnId(id: Long) extends VColumnId

// vim: set ts=4 sw=4 et:
