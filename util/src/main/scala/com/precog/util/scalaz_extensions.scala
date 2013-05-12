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

import scalaz._
import scalaz.StreamT._

package object scalaz_extensions {
  /*
  def mapM[M[_], A, B](stream: StreamT[M, A])(f: A => M[B])(implicit m: Monad[M]): StreamT[M, B] = stepBind(stream) {
    _( yieldd = (a, s) => m.map(f(a)) { Yield(_, mapM(s)(f)) }
     , skip = s => m.point(Skip(mapM(s)(f)))
     , done = m.point(Done)
     )
  }

  private def stepBind[M[_], A, B](stream: StreamT[M, A])(f: Step[A, StreamT[M, A]] => M[Step[B, StreamT[M, B]]])(implicit M: Monad[M]): StreamT[M, B] = 
    StreamT(M.bind(stream.step)(f))
    */
}
