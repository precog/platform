package com.reportgrid.storage

import com.reportgrid.analytics.Path
import blueeyes.json._
import blueeyes.json.JsonAST._

import java.nio.ByteBuffer

import scalaz.effect._
import scalaz.iteratee._

sealed trait Sync {
  def sync: IO[Unit]
}

trait Projection extends Sync {
  def insert(id : Long, v : ByteBuffer, shouldSync: Boolean = false): IO[Unit]

  def getAllIds[F[_] : MonadIO, A] : EnumeratorT[Unit, Long, F, A]
  def getIdsInRange[F[_] : MonadIO, A](range : Interval[Long]): EnumeratorT[Unit, Long, F, A]
  def getIdsForValue[F[_] : MonadIO, A](v : ByteBuffer): EnumeratorT[Unit, Long, F, A]
  def getIdsByValueRange[F[_] : MonadIO, A](range : Interval[ByteBuffer]): EnumeratorT[Unit, Long, F, A]

  def getAllValues[F[_] : MonadIO, A] : EnumeratorT[Unit, ByteBuffer, F, A]
  def getValuesInRange[F[_]: MonadIO, A](range: Interval[ByteBuffer]): EnumeratorT[Unit, ByteBuffer, F, A]
  def getValueForId[F[_]: MonadIO, A](id: Long): EnumeratorT[Unit, ByteBuffer, F, A]
  def getValuesByIdRange[F[_]: MonadIO, A](range: Interval[Long]): EnumeratorT[Unit, ByteBuffer, F, A]
}

trait LengthEncoder {
  def fixedLength(d: BigInt): Option[Int]
  def fixedLength(s: String): Option[Int]
}

sealed trait ColumnType 


object ColumnType {
  case object Long    extends ColumnType 
  case object Double  extends ColumnType 
  case object Boolean extends ColumnType 
  case object Null    extends ColumnType
  case object Nothing extends ColumnType
  case class BigDecimal(width: Option[Int]) extends ColumnType 
  case class String(width: Option[Int]) extends ColumnType 

  def forValue(jvalue: JValue)(implicit lengthEncoder: LengthEncoder) : Option[ColumnType] = jvalue match {
    case JBool(_)        => Some(Boolean)
    case JInt(value)     => Some(BigDecimal(lengthEncoder.fixedLength(value)))
    case JDouble(_)      => Some(Double)
    case JString(value)  => Some(String(lengthEncoder.fixedLength(value)))
    case _ => None
  }
}

case class ColumnDescriptor(selector: JPath, columnType: ColumnType)

case class ColumnMetadata(idRange: Interval[Long], dataRange: Interval[JValue])

trait ProjectionDescriptor extends Sync {
  def path: Path
  def columns: List[ColumnDescriptor]
  def sortDepth: Int
}

trait ProjectionMetadata extends Sync {
  def columnMetadata: Map[ColumnDescriptor, ColumnMetadata]
}
