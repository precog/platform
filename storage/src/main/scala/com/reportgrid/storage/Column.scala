package com.reportgrid.storage

import scalaz.effect._
import scalaz.iteratee._
import java.nio.ByteBuffer

trait Column {
  def getAllIds[F[_] : MonadIO, A] : EnumeratorT[Unit, Long, F, A]
  def getIdsInRange[F[_] : MonadIO, A](range : Interval[Long]): EnumeratorT[Unit, Long, F, A]
  def getIdsForValue[F[_] : MonadIO, A](v : ByteBuffer): EnumeratorT[Unit, Long, F, A]
  def getIdsByValueRange[F[_] : MonadIO, A](range : Interval[ByteBuffer]): EnumeratorT[Unit, Long, F, A]

  def getAllValues[F[_] : MonadIO, A] : EnumeratorT[Unit, ByteBuffer, F, A]
  def getValuesInRange[F[_]: MonadIO, A](range: Interval[ByteBuffer]): EnumeratorT[Unit, ByteBuffer, F, A]
  def getValueForId[F[_]: MonadIO, A](id: Long): EnumeratorT[Unit, ByteBuffer, F, A]
  def getValuesByIdRange[F[_]: MonadIO, A](range: Interval[Long]): EnumeratorT[Unit, ByteBuffer, F, A]
}

