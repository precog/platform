package com.reportgrid.yggdrasil

import com.reportgrid.util._
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
  def getAllPairs[X] : EnumeratorP[X, (Identities, Seq[CValue]), IO]
  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, (Identities, Seq[CValue]), IO]
  def getPairForId[X](id: Identities): EnumeratorP[X, (Identities, Seq[CValue]), IO]

  def getAllIds[X] : EnumeratorP[X, Identities, IO] 
  def getIdsInRange[X](range : Interval[Identities]) : EnumeratorP[X, Identities, IO] 

  def getAllValues[X] : EnumeratorP[X, Seq[CValue], IO] 
  def getValuesByIdRange[X](range: Interval[Identities]) : EnumeratorP[X, Seq[CValue], IO]
  def getValueForId[X](id: Identities): EnumeratorP[X, Seq[CValue], IO] 
}
