package com.precog.yggdrasil
package table

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

sealed trait VColumnId 
case class NamedColumnId(path: Path, selector: JPath) extends VColumnId
case class DynColumnId(id: Long) extends VColumnId

// vim: set ts=4 sw=4 et:
