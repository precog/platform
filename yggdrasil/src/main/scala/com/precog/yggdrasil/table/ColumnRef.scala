package com.precog.yggdrasil
package table

import blueeyes.json.JPath
import com.precog.common.Path

case class ColumnRef(selector: JPath, ctype: CType) {
  type CA = ctype.CA
}


// vim: set ts=4 sw=4 et:
