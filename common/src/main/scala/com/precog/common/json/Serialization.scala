/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.common.json

import blueeyes.json.JPath
import blueeyes.json._
import blueeyes.json.serialization.{ Decomposer, Extractor }
import blueeyes.json.serialization.IsoSerialization._
import Extractor._

import shapeless._
import scalaz._
import scalaz.Validation._

object Serialization {
  def versioned[A](extractor: Extractor[A], version: Option[String]): Extractor[A] = new Extractor[A] {
    def validated(jv: JValue) = {
      if (version.forall(v => (jv \ "schemaVersion") == JString(v))) {
        extractor.validated(jv)
      } else {
        failure(Invalid("Record schema " + (jv \ "schemaVersion") + " of value " + jv.renderCompact + " does not conform to version " + version))
      }
    }
  }

  def versioned[A](decomposer: Decomposer[A], version: Option[String]): Decomposer[A] = new Decomposer[A] {
    def decompose(a: A): JValue = {
      val baseResult = decomposer.decompose(a)
      version map { v =>
       if (baseResult.isInstanceOf[JObject]) {
          baseResult.unsafeInsert(JPath(".schemaVersion"), JString(v))
        } else {
          sys.error("Cannot version primitive or array values!")
        }
      } getOrElse {
        baseResult
      }
    }
  }
}

class MkDecomposerV[T] {
  def apply[F <: HList, L <: HList](fields: F, version: Option[String])(implicit iso: Iso[T, L], decomposer: DecomposerAux[F, L]): Decomposer[T] =
    Serialization.versioned(new IsoDecomposer(fields, iso, decomposer), version)
}

class MkExtractorV[T] {
  def apply[F <: HList, L <: HList](fields: F, version: Option[String])(implicit iso: Iso[T, L], extractor: ExtractorAux[F, L]): Extractor[T] =
    Serialization.versioned(new IsoExtractor(fields, iso, extractor), version)
}

class MkSerializationV[T] {
  def apply[F <: HList, L <: HList](fields: F, version: Option[String])
    (implicit iso: Iso[T, L], decomposer: DecomposerAux[F, L], extractor: ExtractorAux[F, L]): (Decomposer[T], Extractor[T]) =
      (Serialization.versioned(new IsoDecomposer(fields, iso, decomposer), version),
       Serialization.versioned(new IsoExtractor(fields, iso, extractor), version))
}
