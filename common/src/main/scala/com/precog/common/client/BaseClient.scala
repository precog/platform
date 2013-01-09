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
