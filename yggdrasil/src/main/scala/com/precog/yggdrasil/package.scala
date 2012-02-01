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
package com.precog

package object yggdrasil {
  type Identity = Long
  type Identities = Seq[Identity]
  type SEvent = (Identities, SValue)

  object SEvent {
    def apply(id: Identities, sv: SValue): SEvent = (id, sv)
  }

  implicit object IdentitiesOrdering extends Ordering[Identities] {
    def compare(id1: Identities, id2: Identities) = {
      (id1 zip id2).foldLeft(0) {
        case (acc, (i1, i2)) => if (acc != 0) acc else implicitly[Ordering[Identity]].compare(i1, i2)
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
