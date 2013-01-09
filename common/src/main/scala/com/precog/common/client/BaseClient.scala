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
package com.precog.common
package client

import akka.dispatch.Future
import scalaz._

trait BaseClient {
  /**
   * A client's monad is, essentially, a future. However, there is a
   * chance that the server could be doing wonky things, so we create a side
   * channel, the left side of the EitherT, for reporting errors related
   * strictly to unexpected communication issues with the server.
   */
  type Response[+A] = EitherT[Future, String, A]

  def Response[A](a: Future[A])(implicit F: Functor[Future]): Response[A] = EitherT.right(a)
  def BadResponse(msg: String)(implicit F: Pointed[Future]): Response[Nothing] = EitherT.left(F.point(msg))

  case class ClientException(message: String) extends Exception(message)

  /**
   * A natural transformation from Response to Future that maps the left side
   * to exceptions thrown inside the future.
   */
 implicit def ResponseAsFuture(implicit F: Functor[Future]) = new (Response ~> Future) {
    def apply[A](res: Response[A]): Future[A] = res.fold({ error =>
      throw ClientException(error)
    }, identity)
  }

  implicit def FutureAsResponse(implicit F: Pointed[Future]) = new (Future ~> Response) {
    def apply[A](fa: Future[A]): Response[A] = EitherT.eitherT(fa.map(\/.right).recoverWith {
      case ClientException(msg) => F.point(\/.left[String, A](msg))
    })
  }
}


// vim: set ts=4 sw=4 et:
