package com.querio.daze

case class IdentitySource(sources: Set[ProjectionDescriptor])
case class EventMatcher(order: Order[DEvent], merge: (Vector[Identity], Vector[Identity]) => Vector[Identity])

case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], identityDerivation: Vector[IdentitySource])

// QualifiedSelector(path: String, sel: JPath, valueType: EType)
trait StorageEngineInsertAPI

trait StorageEngineQueryAPI {
  def column(path: Path): DatasetEnum[X, DEvent, IO]

  //def column(path: String, selector: JPath, valueType: EType): DatasetEnum[X, DEvent, IO]
  //def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: EType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait StorageEngineAPI extends StorageEngineInsertAPI with StorageEngineQueryAPI 


trait OperationsAPI {
  def cogroup[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F])(implicit matcher: EventMatcher): DatasetEnum[X, Either3[DEvent, (DEvent, DEvent), DEvent], F]

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F]): DatasetEnum[X, DEvent, F]

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F]): DatasetEnum[X, DEvent, F]

  def join[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F])(implicit matcher: EventMatcher): DatasetEnum[X, (DEvent, DEvent), F]

  def merge[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F])(implicit matcher: EventMatcher): DatasetEnum[X, DEvent, F]

  def sort[X, F[_]: Monad](enum: DatasetEnum[X, DEvent, F], identityIndex: Int): DatasetEnum[X, DEvent, F]

  def vmap[X, F[_]: Monad](enum: DatasetEnum[X, DEvent, F])(f: SValue => SValue): DatasetEnum[X, DEvent, F]
  def pmap[X, F[_]: Monad](enum: DatasetEnum[X, (DEvent, DEvent), F])(f: (SValue, SValue) => SValue): DatasetEnum[X, DEvent, F]

  def filter[X, F[_]: Monad](enum: DatasetEnum[X, (DEvent, DEvent), F])(pred: (SValue, SValue) => Option[SValue]): DatasetEnum[X, DEvent, F]

  def lift[X, F[_]: Monad](value: SValue): DatasetEnum[X, DEvent, F]
}
