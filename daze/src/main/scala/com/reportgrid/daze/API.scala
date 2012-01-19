package com.querio.daze

import com.reportgrid.common.json._
import com.reportgrid.analytics.Path
import scalaz._
import scalaz.effect._
import scalaz.iteratee._

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


trait DatasetEnumInstances {
  def cogroup[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, Either3[SEvent, (SEvent, SEvent), SEvent], F]

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, SEvent, F]

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, SEvent, F]

  def join[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, (SEvent, SEvent), F]

  def merge[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F]

  def sort[X, F[_]: Monad](enum: DatasetEnum[X, SEvent, F], identityIndex: Int): DatasetEnum[X, SEvent, F]
  
  def map[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => E2): DatasetEnum[X, E2, F]

  def filter[X, F[_]: Monad](enum: DatasetEnum[X, (SEvent, SEvent), F])(pred: (SValue, SValue) => Option[SValue]): DatasetEnum[X, SEvent, F]

  def point[X, F[_]: Monad](value: SEvent): DatasetEnum[X, SEvent, F]

  def lift(value: SValue): SEvent

  def flatMap[X, E1, E2,  F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F]): DatasetEnum[X, E2, F]
}

trait OperationsAPI {
  def ops: DatasetEnumInstances
}

