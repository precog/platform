package com.querio.daze

import com.reportgrid.yggdrasil._
import com.reportgrid.yggdrasil.util.Enumerators
import com.reportgrid.analytics.Path
import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._

import EnumeratorP._

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

  def sort[X, F[_]](enum: DatasetEnum[X, SEvent, F], identityIndices: Vector[Int])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F]
  
  def map[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => E2): DatasetEnum[X, E2, F] = DatasetEnum(
    enum.enum.mapE[E2] {
      new ForallM[({ type λ[β[_], α] = EnumerateeT[X, E1, E2, β, α] })#λ] {
        def apply[FF[_]: Monad, A] = EnumerateeT.map[X, E1, E2, FF, A](f)
      }
    }
  )

  def empty[X, E, F[_]: Monad]: DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans, A] = {
        type FG[α] = F[G, α]
        Empty[({ type λ[α] = EnumeratorT[X, E, FG, α]].empty[A]
      }
    }
  )


  def point[X, E, G[_]: Monad](value: E): DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans, A] = EnumeratorT.enumOne[X, E, ({ type λ[α] = F[G, α] })#λ, A](value)(MonadTrans[F].apply[G])
    }
  )

  def liftM[X, E, F[_]: Monad](value: F[E]): DatasetEnum[X, E, F]

  def flatMap[X, E1, E2,  F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F]): DatasetEnum[X, E2, F]
  
  def mapOpt[X, E1, E2,  F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => Option[E2]): DatasetEnum[X, E2, F]
}

trait OperationalDatasetEnumFunctions {
  def sort[X, F[_]](enum: DatasetEnum[X, SEvent, F], identityIndices: Vector[Int])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F] =
    sys.error("todo")
    //DatasetEnum(Enumerators.sort(enum.enum))
}

trait OperationsAPI {
  def query: StorageEngineQueryAPI
  def ops: DatasetEnumFunctions
}

