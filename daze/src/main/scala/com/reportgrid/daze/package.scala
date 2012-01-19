package com.querio

import com.reportgrid.common.json._

package object daze {
  type Identity = Long
  type Identities = Vector[Identity]
  type SEvent = (Identities, SValue)
}



// vim: set ts=4 sw=4 et:
