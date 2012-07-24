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
package com.precog.yggdrasil.test

import scalaz._


// An identity monad, since using scalaz's Id causes problem in for-comprehensions
// when A is structurally a Scala monad with flatMap et. al.
case class YId[+A](a: A)

trait YIdInstances {
  implicit val M: Monad[YId] with Copointed[YId] = new Monad[YId] with Copointed[YId] {
    def point[A](a: => A) = YId(a)
    def bind[A, B](m: YId[A])(f: A => YId[B]) = f(m.a)
    def copoint[A](y: YId[A]) = y.a
  }

  implicit val coM: Copointed[YId] = M
}

object YId extends YIdInstances


// vim: set ts=4 sw=4 et:
