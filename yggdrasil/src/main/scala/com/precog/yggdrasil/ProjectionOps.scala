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

import scalaz._
import scalaz.Scalaz._
import scalaz.iteratee._
import scalaz.iteratee.IterateeT._
import scalaz.iteratee.StepT._
import scalaz.iteratee.Input._
import scalaz.iteratee.EnumeratorT._
import scalaz.iteratee.EnumerateeT._
import scalaz.effect._
import scalaz.syntax.order._
import scalaz.syntax.bind._
import java.nio.ByteBuffer

object ProjectionOps {
  def printer[X, E, F[_] : Monad] : IterateeT[X, E, F, Unit] = {
    def loop : Input[E] => IterateeT[X, E, F, Unit] = {
      i => i.fold(el = {e => println(e); cont(loop)},
                  empty = cont(loop),
                  eof = {println("All done"); done((), eofInput)})
    }

    cont(loop)
  }

  def sumSValues[X, F[_]: Monad]: IterateeT[X, SValue, F, Option[SValue]] = {
    foldM[X, SValue, F, Option[SValue]](None) { (acc, sv) => 
      Monad[F].point {
        acc map { asv =>
          sv.mapDoubleOr(asv) {
            d => SDouble(asv.mapDoubleOr(d) { _ + d })
          }
        } orElse {
          sv.mapDoubleOr(Option.empty[SValue]) { _ => Some(sv) }
        }
      }
    }
  }


  // enumE((sumSValues >>== enum).run(x => Monad[F].point(None)))
}
