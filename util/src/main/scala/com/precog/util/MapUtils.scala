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
package com.precog.util

import scala.collection.{GenTraversable, GenMap}
import scala.collection.generic.CanBuildFrom

import scalaz.{Either3, Left3, Right3, Middle3}

/**
 * Implicit container trait
 */
trait MapUtils {
  implicit def pimpMapUtils[A, B, CC[B] <: GenTraversable[B]](self: GenMap[A, CC[B]]): MapPimp[A, B, CC] =
    new MapPimp(self)
}

class MapPimp[A, B, CC[B] <: GenTraversable[B]](left: GenMap[A, CC[B]]) {
  def cogroup[C, CC2[C] <: GenTraversable[C], Result](right: GenMap[A, CC2[C]])(implicit cbf: CanBuildFrom[Nothing, (A, Either3[B, (CC[B], CC2[C]), C]), Result], cbfLeft: CanBuildFrom[CC[B], B, CC[B]], cbfRight: CanBuildFrom[CC2[C], C, CC2[C]]): Result = {
    val resultBuilder = cbf()
    
    left foreach {
      case (key, leftValues) => {
        right get key map { rightValues =>
          resultBuilder += (key -> Either3.middle3[B, (CC[B], CC2[C]), C]((leftValues, rightValues)))
        } getOrElse {
          leftValues foreach { b => resultBuilder += (key -> Either3.left3[B, (CC[B], CC2[C]), C](b)) }
        }
      }
    }
    
    right foreach {
      case (key, rightValues) => {
        if (!(left get key isDefined)) {
          rightValues foreach { c => resultBuilder += (key -> Either3.right3[B, (CC[B], CC2[C]), C](c)) }
        }
      }
    }
    
    resultBuilder.result()
  }
}
