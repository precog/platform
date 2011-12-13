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
package com.reportgrid.storage

trait StorageEngine {
  /** Streams values, by order of identity.
   */
  def stream[I: Reified, A: Reified](dataset: Expr[Dataset[I, A]], cb: Set[(I, A)] => Boolean): Unit

  def reduce[I: Reified, A: Reified, Z](dataset: Expr[Dataset[I, A]], state: Z)(f: (Z, A) => Z): Z

  def save[A: Reified, IA, B: Reified, IB: Reified]
    (dataset: Expr[Dataset[IA, A]], f: (IA, A) => (IB, B), target: DatasetId): Unit

  def insert[A: Reified](dataset: DatasetId, values: Set[A]): Unit

  def insert[A: Reified, I: Reified](target: DatasetId, values: Set[(I, A)]): Unit

  def move[A: Reified, I: Reified](source: DatasetId, target: DatasetId): Unit

  def delete[A: Reified, I: Reified](source: DatasetId): Unit

  def get[A: Reified, I: Reified](source: DatasetId, ids: Set[I]): Set[(I, A)]
}