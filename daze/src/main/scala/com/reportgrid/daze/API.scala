package com.querio
package daze

import com.reportgrid.yggdrasil._
import com.reportgrid.yggdrasil.util.Enumerators
import com.reportgrid.analytics.Path
import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._

import Iteratee._

//case class IdentitySource(sources: Set[ProjectionDescriptor])
case class EventMatcher(order: Order[SEvent], merge: (Vector[Identity], Vector[Identity]) => Vector[Identity])

case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F]) //, identityDerivation: Vector[IdentitySource]) 

// QualifiedSelector(path: String, sel: JPath, valueType: EType)
trait StorageEngineInsertAPI

trait StorageEngineQueryAPI {
  def fullProjection[X](path: Path): DatasetEnum[X, SEvent, IO]

  //def column(path: String, selector: JPath, valueType: EType): DatasetEnum[X, SEvent, IO]
  //def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: EType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait StorageEngineAPI extends StorageEngineInsertAPI with StorageEngineQueryAPI 

trait DatasetEnumFunctions {
  def cogroup[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, Either3[SEvent, (SEvent, SEvent), SEvent], F] = 
    DatasetEnum(cogroupE[X, SEvent, F].apply(enum1.enum, enum2.enum))

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] =
    DatasetEnum(crossE(enum1.enum, enum2.enum))

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] =
    DatasetEnum(crossE(enum2.enum, enum1.enum))

  def join[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, (SEvent, SEvent), F] =
    DatasetEnum(matchE[X, SEvent, F].apply(enum1.enum, enum2.enum))

  def merge[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F] =
    DatasetEnum(mergeE[X, SEvent, F].apply(enum1.enum, enum2.enum))

  def sort[X, F[_]](enum: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F]
  
  def map[X, E1, E2, G[_]: Monad](enum: DatasetEnum[X, E1, G])(f: E1 => E2): DatasetEnum[X, E2, G] = DatasetEnum(
    new EnumeratorP[X, E2, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] = new EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] {
        def apply[A] = enum.enum[F].map(f)(MonadTrans[F].apply[G]).apply[A]
      }
    }
  )

  def empty[X, E, G[_]: Monad]: DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E, ({ type λ[α] = F[G, α] })#λ] = {
        type FG[a] = F[G, a]
        implicit val FMonad = MonadTrans[F].apply[G]
        Monoid[EnumeratorT[X, E, FG]].zero
      }
    }
  )

  def point[X, E, G[_]: Monad](value: E): DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans] = EnumeratorT.enumOne[X, E, ({ type λ[α] = F[G, α] })#λ](value)(MonadTrans[F].apply[G])
    }
  )

  def liftM[X, E, G[_]: Monad](value: G[E]): DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E, ({ type λ[α] = F[G, α] })#λ] = new EnumeratorT[X, E, ({ type λ[α] = F[G, α] })#λ] {
        type FG[a] = F[G, a]
        implicit val FMonad = MonadTrans[F].apply[G]

        def apply[A] = { (step: StepT[X, E, FG, A]) => 
          iterateeT[X, E, FG, A](FMonad.bind(MonadTrans[F].liftM(value)) { e => step.mapCont(f => f(elInput(e))).value })
        }
      }
    }
  )

  def flatMap[X, E1, E2, G[_]: Monad](enum: DatasetEnum[X, E1, G])(f: E1 => DatasetEnum[X, E2, G]): DatasetEnum[X, E2, G] = DatasetEnum(
    new EnumeratorP[X, E2, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] = {
        implicit val FMonad = MonadTrans[F].apply[G]
        enum.enum[F].flatMap(e => f(e).enum[F])
      }
    }
  )
  
  def collect[X, E1, E2, G[_]: Monad](enum: DatasetEnum[X, E1, G])(pf: PartialFunction[E1, E2]): DatasetEnum[X, E2, G] = DatasetEnum(
    new EnumeratorP[X, E2, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] = {
        implicit val FMonad = MonadTrans[F].apply[G]
        enum.enum[F].collect(pf)
      }
    }
  )
}

trait OperationsAPI {
  def query: StorageEngineQueryAPI
  def ops: DatasetEnumFunctions
}

