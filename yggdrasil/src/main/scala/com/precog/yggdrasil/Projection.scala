package com.precog.yggdrasil

import com.precog.util._
import com.precog.analytics.Path

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
  def descriptor: ProjectionDescriptor

  def chunkSize: Int

  def getAllPairs[X] : EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO]

  def getAllIds[X] : EnumeratorP[X, Vector[Identities], IO] = getAllPairs map(_.map { case (id, _) => id })
  def getAllValues[X] : EnumeratorP[X, Vector[Seq[CValue]], IO] = getAllPairs map( _.map { case (_, b) => b })
  def getColumnValues[X](path: Path, selector: JPath): EnumeratorP[X, Vector[(Identities, CValue)], IO] = {
    val columnIndex = descriptor.columns.indexWhere(col => col.path == path && col.selector == selector)
    getAllPairs map( _.map { case (id, b) => (id, b(columnIndex)) })
  }

  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO]

  /**
   * Retrieve all IDs for IDs in the given range [start,end]
   */  
  def getIdsInRange[X](range : Interval[Identities]) : EnumeratorP[X, Vector[Identities], IO] =
    getPairsByIdRange(range) map( _.map { case (id, _) => id })

  /**
   * Retrieve all values for IDs in the given range [start,end]
   */  
  def getValuesByIdRange[X](range: Interval[Identities]) : EnumeratorP[X, Vector[Seq[CValue]], IO] =
    getPairsByIdRange(range) map( _.map { case (_, b) => b })

  def getPairForId[X](id: Identities): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = 
    getPairsByIdRange(Interval(Some(id), Some(id)))

  def getValueForId[X](id: Identities): EnumeratorP[X, Vector[Seq[CValue]], IO] = 
    getValuesByIdRange(Interval(Some(id), Some(id)))
}
