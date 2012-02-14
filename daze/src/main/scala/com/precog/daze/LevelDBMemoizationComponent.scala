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
package daze

import yggdrasil._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import IterateeT._

trait LevelDBMemoizationConfig {
  def memoizationBufferSize: Int
  def memoizationWorkDir: File
}

trait LevelDBMemoizationComponent extends YggConfigComponent with MemoizationComponent { component => 
  type YggConfig <: LevelDBMemoizationConfig

  private var cache = Map.empty[Int, (Option[ProjectionDescriptor], Either[Vector[SEvent], File])]

  class MemoContext[X] extends MemoizationContext[X] { 
    def apply(memoId: Int)(implicit asyncContext: ExecutionContext): Either[Memoizer[X], DatasetEnum[X, SEvent, IO]] = component.synchronized {
      cache.get(memoId) match {
        case Some((descriptor, Left(vector))) => 
          Right(DatasetEnum(Future(EnumeratorP.enumPStream[X, SEvent, IO](vector.toStream)), descriptor))
          
        case Some((descriptor, Right(file))) => 
          sys.error("disk-based memoization not yet supported") 

        case None => Left(
          new Memoizer[X] {
            def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = (iter: IterateeT[X, SEvent, F, A]) => {
              import MO._
              (iter zip (fold[X, SEvent, F, Vector[SEvent]](Vector.empty[SEvent]) { (v, ev) => v :+ ev })) map {
                case (result, vector) => 
                  component.synchronized { if (!cache.isDefinedAt(memoId)) cache += (memoId -> (d, Left(vector))) }
                  result
              }
            }
          }
        )
      }
    }
  }
}




// vim: set ts=4 sw=4 et:
