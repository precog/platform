package com.reportgrid.storage

import com.reportgrid.analytics.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import java.nio.ByteBuffer

import scalaz._
import scalaz.Scalaz._
import scalaz.effect._
import scalaz.iteratee._

trait Sync[A] {
  def read: Option[IO[Validation[String, A]]]
  def sync(a: A): IO[Validation[Throwable,Unit]]
}

trait Projection {
  def insert(id : Long, v : ByteBuffer, shouldSync: Boolean = false): IO[Unit]

  def getAllPairs[X] : EnumeratorP[X, (Long,ByteBuffer), IO]
  def getPairsByIdRange[X](range: Interval[Long]): EnumeratorP[X, (Long,ByteBuffer), IO]
  def getPairForId[X](id: Long): EnumeratorP[X, (Long,ByteBuffer), IO]

  def getAllIds[X] : EnumeratorP[X, Long, IO]
  def getIdsInRange[X](range : Interval[Long]): EnumeratorP[X, Long, IO]

  def getAllValues[X] : EnumeratorP[X, ByteBuffer, IO]
  def getValuesByIdRange[X](range: Interval[Long]): EnumeratorP[X, ByteBuffer, IO]
  def getValueForId[X](id: Long): EnumeratorP[X, ByteBuffer, IO]
}
