package com.precog.yggdrasil

import blueeyes.json.JPath
import com.precog.common.Path

sealed trait ColumnRef {
  type CA
}

case class IColumnRef(idx: Int) extends ColumnRef {
  type CA = Long
}

class VColumnRef private (val id: VColumnId, val ctype: CType) {
  type CA = ctype.CA
}

object VColumnRef {
  @inline def apply[A](id: VColumnId, ctype: CType { type CA = A }): VColumnRef { type CA = A } = {
    new VColumnRef(id, ctype).asInstanceOf[VColumnRef { type CA = A }]
  }

  def unapply(ref: VColumnRef): Option[(VColumnId, CType)] = {
    Some((ref.id, ref.ctype))
  }

  @inline def cast[A](ref: VColumnRef) = ref.asInstanceOf[VColumnRef { type CA = A }]
}

sealed trait VColumnId 
case class NamedColumnId(path: Path, selector: JPath) extends VColumnId
case class DynColumnId(id: Long) extends VColumnId

// vim: set ts=4 sw=4 et:
