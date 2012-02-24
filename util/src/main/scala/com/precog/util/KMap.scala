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

trait KMap[K[_], V[_]] {
  def +[A](kv: (K[A], V[A])): KMap[K, V]
  def -[A](k: K[A]): KMap[K, V]
  def get[A](k: K[A]): Option[V[A]]
  def isDefinedAt[A](k: K[A]): Boolean
}

object KMap {
  private case class CastMap[K[_], V[_]](delegate: Map[Any, Any]) extends KMap[K, V] {
    def +[A](kv: (K[A], V[A])): KMap[K, V] = CastMap(delegate + kv)
    def -[A](k: K[A]): KMap[K, V] = CastMap(delegate - k)
    def get[A](k: K[A]): Option[V[A]] = delegate.get(k).map(_.asInstanceOf[V[A]])
    def isDefinedAt[A](k: K[A]): Boolean = delegate.isDefinedAt(k)
  }

  def empty[K[_], V[_]]: KMap[K, V] = CastMap(Map())
}

// vim: set ts=4 sw=4 et:
