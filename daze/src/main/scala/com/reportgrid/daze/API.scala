package com.reportgrid.daze

// QualifiedSelector(path: String, sel: JPath, valueType: ValueType)

type RawEvent = (Seq[Long], ByteBuffer)

trait StorageEngineInsertAPI {
}

trait StorageEngineQueryAPI {
  def column(path: String, selector: JPath, valueType: ValueType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]

  def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: ValueType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait StorageEngineAPI extends StorageEngineInsertAPI with StorageEngineQueryAPI

// ProjectionDescriptor describes PHYSICAL FORMAT of values in the byte buffer
case class ValuesDescriptor(values: Seq[(ValueFormat, Set[Metadata])])
case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], identityDesc: Seq[Set[ColumnDescriptor]], valuesDesc: ValuesDescriptor)

trait OperationsAPI {
  def cogroup[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, Either3[RawEvent, RawEvent, RawEvent], F]

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, RawEvent, F]

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, RawEvent, F]

  def matched[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, RawEvent, F]

  def merge[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, Either[RawEvent, RawEvent], F]

  def sortIdentity[X, F[_]: Monad](enum: DatasetEnum[X, RawEvent, F], identityIndex: Int): DatasetEnum[X, RawEvent, F]

  def mapValues[X, F[_]: Monad](enum: DatasetEnum[X, RawEvent, F], d: (ValueDescriptor, ByteBuffer => ByteBuffer)): DatasetEnum[X, RawEvent, F]

  def mapIdentities[X, F[_]: Monad](enum: DatasetEnum[X, RawEvent, F], d: (Seq[Set[ColumnDescriptor]], Seq[Long] => Seq[Long])): DatasetEnum[X, RawEvent, F]
}

  
  


// vim: set ts=4 sw=4 et:
