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

sealed trait Unapply[A, B] {
  def unapply(b: B): A
}

trait Bijection[A, B] extends Function1[A, B] with Unapply[A, B] 

sealed class Biject[A](a: A) {
  def as[B](implicit f: Either[Bijection[A, B], Bijection[B, A]]): B = f match {
    case lbf: Left[_,_] => lbf.a.apply(a)
    case rbf: Right[_,_] => rbf.b.unapply(a)
  }
}

trait Bijections {
  implicit def biject[A](a: A): Biject[A] = new Biject(a)
  implicit def forwardEither[A, B](implicit a: Bijection[A,B]): Either[Bijection[A,B], Bijection[B,A]] = Left(a)
  implicit def reverseEither[A, B](implicit b: Bijection[B,A]): Either[Bijection[A,B], Bijection[B,A]] = Right(b)
}

object Bijection extends Bijections 
