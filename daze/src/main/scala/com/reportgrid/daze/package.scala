package com.querio

package object daze {
  type Identity = Long
  type Identities = Vector[Identity]
  type DEvent = (Identities, SValue)
}



// vim: set ts=4 sw=4 et:
