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
package yggdrasil
package serialization

import java.io._
import java.util.zip._
import scala.annotation.tailrec
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Iteratee._


trait FileSerialization[E] extends StreamSerialization {
  def writeElement(out: DataOutputStream, ev: E): IO[Unit]
  def readElement(in: DataInputStream): IO[Option[E]]
}

object FileSerialization {
  def noop[E]: FileSerialization[E] = new FileSerialization[E] with ZippedStreamSerialization {
    def writeElement(out: DataOutputStream, ev: E): IO[Unit] = IO(())
    def readElement(in: DataInputStream): IO[Option[E]] = IO(Option.empty[E])
  }
}

trait IterateeFileSerialization[E] extends FileSerialization[E] {
  def reader[X](file: File): EnumeratorP[X, E, IO] = new EnumeratorP[X, E, IO] {
    def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, E, G] = {
      import MO._
      new EnumeratorT[X, E, G] {
        def apply[A] = {
          def step(in: DataInputStream): StepT[X, E, G, A] => IterateeT[X, E, G, A] = s => s mapContOr(
            k => iterateeT(MO.promote(readElement(in)) flatMap {
                   case Some(ev) => (k(elInput(ev)) >>== step(in)).value
                   case None     => (k(eofInput) >>== step(in)).value
                 }), 
            iterateeT(MO.promote(IO(in.close)) >> s.pointI.value)
          )

          (s: StepT[X, E, G, A]) => step(iStream(file))(s)
        }
      }
    }
  }

  def writer[X, F[_]](file: File)(implicit MO: F |>=| IO): IterateeT[X, E, F, File] = {
    import MO._
    def dump(outIO: F[DataOutputStream]): IterateeT[X, E, F, File] = 
      cont(
        (_: Input[E]).fold(
          el    = el => dump(for { out  <- outIO; _ <- MO.promote(writeElement(out, el)) } yield out),
          empty = dump(outIO),
          eof   = iterateeT(
            for {
              out <- outIO
              _   <- MO.promote(IO(out.close))
            } yield sdone[X, E, F, File](file, eofInput))))

    dump(MO.promote(IO(oStream(file))))
  }
}

