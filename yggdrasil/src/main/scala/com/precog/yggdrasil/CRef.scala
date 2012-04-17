package com.precog.yggdrasil

import blueeyes.json.JPath
import com.precog.common.Path

sealed trait CRef
case class CPaths(path: Path, selector: JPath) extends CRef
case class CDyn(id: Long) extends CRef

case class CMeta(cref: CRef, ctype: CType) {
  type CA = ctype.CA
}

// vim: set ts=4 sw=4 et:
