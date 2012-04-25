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
package table

trait CogroupMerge {
  def apply[@specialized(Boolean, Long, Double) A](ref: VColumnRef[A]): Option[F2P[A, A, A]]
}

object CogroupMerge {
  object second extends CogroupMerge {
    def apply[@specialized(Boolean, Long, Double) A](ref: VColumnRef[A]): Option[F2P[A, A, A]] = {
      Some(
        new F2P[A, A, A] { 
          val returns = ref.ctype
          val accepts = (ref.ctype, ref.ctype)
          def isDefinedAt(a1: A, a2: A) = true
          def apply(a1: A, a2: A) = a2 
        }
      )
    }
  }
}

// vim: set ts=4 sw=4 et:
