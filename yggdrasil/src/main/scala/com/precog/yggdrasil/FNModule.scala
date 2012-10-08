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

trait FNModule {
  type F1 
  type F2

  implicit def liftF1(f1: F1): F1Like 
  trait F1Like {
    def compose(f1: F1): F1
    def andThen(f1: F1): F1
  }

  implicit def liftF2(f2: F2): F2Like 
  trait F2Like {
    def applyl(cv: CValue): F1 
    def applyr(cv: CValue): F1

    def andThen(f1: F1): F2
  }
}

trait FNDummyModule extends FNModule {
  type F1 = table.CF1
  type F2 = table.CF2

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = new table.CF1(f(table.Column.const(cv), _))
    def applyr(cv: CValue) = new table.CF1(f(_, table.Column.const(cv)))

    def andThen(f1: F1) = new table.CF2((c1, c2) => f(c1, c2) flatMap f1.apply)
  }
}

// vim: set ts=4 sw=4 et:
