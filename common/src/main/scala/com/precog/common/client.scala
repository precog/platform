package com.precog.common

import akka.dispatch.Future
import scalaz._

package object client {
  /**
   * A client's monad is, essentially, a future. However, there is a
   * chance that the server could be doing wonky things, so we create a side
   * channel, the left side of the EitherT, for reporting errors related
   * strictly to unexpected communication issues with the server.
   */
  type ResponseM[M[+_], +A] = EitherT[M, String, A]

  type Response[+A] = ResponseM[Future, A]

  def ResponseMonad(implicit M: Monad[Future]): Monad[Response] = scalaz.EitherT.eitherTMonad[Future, String]

  case class ClientException(message: String) extends Exception(message)

  /**
   * A natural transformation from Response to Future that maps the left side
   * to exceptions thrown inside the future.
   */
  implicit def ResponseAsFuture(implicit M: Monad[Future]) = new (Response ~> Future) {
    def apply[A](res: Response[A]): Future[A] = res.fold({ error => throw ClientException(error) }, identity)
  }

  implicit def FutureAsResponse(implicit M: Monad[Future]) = new (Future ~> Response) {
    def apply[A](fa: Future[A]): Response[A] = EitherT.eitherT(fa.map(\/.right).recoverWith {
      case ClientException(msg) => M.point(\/.left[String, A](msg))
    })
  }

  implicit def FutureStreamAsResponseStream(implicit M: Monad[Future]) = implicitly[Hoist[StreamT]].hoist(FutureAsResponse)
  implicit def ResponseStreamAsFutureStream(implicit MF: Monad[Future], MR: Monad[Response]) = implicitly[Hoist[StreamT]].hoist(ResponseAsFuture)
}
