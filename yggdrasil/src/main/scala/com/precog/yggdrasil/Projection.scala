package com.precog.yggdrasil

import com.precog.util._
import com.precog.common.Path

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
  type X = Throwable
  def read: Option[IO[Validation[X, A]]]
  def sync(a: A): IO[Validation[Throwable,Unit]]
}

trait Projection[Dataset[_]] {
  def descriptor: ProjectionDescriptor

  def chunkSize: Int

  def getAllPairs(expiresAt: Long) : Dataset[Seq[CValue]]
//  def getAllColumnPairs(columnIndex: Int, expiresAt: Long) : Dataset[(Identities, CValue)]
//
//  def getAllIds(expiresAt: Long) : EnumeratorP[X, Vector[Identities], IO]
//  def getAllValues(expiresAt: Long) : EnumeratorP[X, Vector[Seq[CValue]], IO]
//
//  def getColumnValues(path: Path, selector: JPath, expiresAt: Long)(implicit F: Functor[Dataset]): Dataset[CValue] = {
//    @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes
//
//    val columnIndex = descriptor.columns.indexWhere(col => col.path == path && isEqualOrChild(selector, col.selector))
//    getAllPairs(expiresAt).map(_(columnIndex))
//  }

//  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO]
//
//  /**
//   * Retrieve all IDs for IDs in the given range [start,end]
//   */  
//  def getIdsInRange[X](range : Interval[Identities]) : EnumeratorP[X, Vector[Identities], IO] =
//    getPairsByIdRange(range) map( _.map { case (id, _) => id })
//
//  /**
//   * Retrieve all values for IDs in the given range [start,end]
//   */  
//  def getValuesByIdRange[X](range: Interval[Identities]) : EnumeratorP[X, Vector[Seq[CValue]], IO] =
//    getPairsByIdRange(range) map( _.map { case (_, b) => b })
//
//  def getPairForId[X](id: Identities): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = 
//    getPairsByIdRange(Interval(Some(id), Some(id)))
//
//  def getValueForId[X](id: Identities): EnumeratorP[X, Vector[Seq[CValue]], IO] = 
//    getValuesByIdRange(Interval(Some(id), Some(id)))
}
