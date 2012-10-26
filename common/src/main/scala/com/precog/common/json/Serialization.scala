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
import blueeyes.json.JsonAST._
import blueeyes.json.serialization.{ Decomposer, Extractor, ValidatedExtraction }
import Extractor._

import shapeless._
import scalaz._

case object Omit
case object Inline

trait DecomposerAux[F <: HList, L <: HList] {
  def decompose(fields: F, values: L): JValue
}

object DecomposerAux {
  implicit def hnilDecomposer = new DecomposerAux[HNil, HNil] {
    def decompose(fields: HNil, values: HNil) = JObject.empty
  }
  
  implicit def hlistDecomposer1[FT <: HList, H, T <: HList](implicit dh: Decomposer[H], dt: DecomposerAux[FT, T]) =
    new DecomposerAux[String :: FT, H :: T] {
      def decompose(fields: String :: FT, values: H :: T) = 
        // No point propagating decompose validation to the top level, we'd need to throw there anyway
        dt.decompose(fields.tail, values.tail).insert(fields.head, dh.decompose(values.head)).fold(throw _, identity)
    }
  
  implicit def hlistDecomposer2[FT <: HList, H, T <: HList](implicit dh: Decomposer[H], dt: DecomposerAux[FT, T]) =
    new DecomposerAux[String :: FT, Option[H] :: T] {
      def decompose(fields: String :: FT, values: Option[H] :: T) = { 
        val tail = dt.decompose(fields.tail, values.tail)
        // No point propagating decompose validation to the top level, we'd need to throw there anyway
        values.head.map(h => tail.insert(fields.head, dh.decompose(h)).fold(throw _, identity)).getOrElse(tail)
      }
    }
  
  implicit def hlistDecomposer3[FT <: HList, H, T <: HList](implicit dt: DecomposerAux[FT, T]) =
    new DecomposerAux[Omit.type :: FT, Option[H] :: T] {
      def decompose(fields: Omit.type :: FT, values: Option[H] :: T) =
        dt.decompose(fields.tail, values.tail)
    }
  
  implicit def hlistDecomposer4[FT <: HList, H, T <: HList](implicit dh: Decomposer[H], dt: DecomposerAux[FT, T]) =
    new DecomposerAux[Inline.type :: FT, H :: T] {
      def decompose(fields: Inline.type :: FT, values: H :: T) = 
        // No point propagating decompose validation to the top level, we'd need to throw there anyway
        dh.decompose(values.head).insertAll(dt.decompose(fields.tail, values.tail)).fold(l => throw l.head, identity) 
    }
}

trait ExtractorAux[F <: HList, L <: HList] {
  def extract(source: JValue, fields: F): Validation[Error, L]
}

object ExtractorAux {
  implicit val hnilExtractor = new ExtractorAux[HNil, HNil] {
    def extract(source: JValue, fields: HNil) = Success(HNil)
  }
  
  implicit def hlistExtractor1[FT <: HList, H, T <: HList](implicit eh: Extractor[H], et: ExtractorAux[FT, T]) =
    new ExtractorAux[String :: FT, H :: T] {
      def extract(source: JValue, fields: String :: FT) =
        for {
          h <- eh.validated(source \ fields.head)
          t <- et.extract(source, fields.tail)
        } yield h :: t
    }
  
  implicit def hlistExtractor2[FT <: HList, H, T <: HList](implicit et: ExtractorAux[FT, T]) =
    new ExtractorAux[Omit.type :: FT, Option[H] :: T] {
      def extract(source: JValue, fields: Omit.type :: FT) =
        for {
          t <- et.extract(source, fields.tail)
        } yield None :: t
    }
  
  implicit def hlistExtractor3[FT <: HList, H, T <: HList](implicit eh: Extractor[H], et: ExtractorAux[FT, T]) =
    new ExtractorAux[Inline.type :: FT, H :: T] {
      def extract(source: JValue, fields: Inline.type :: FT) =
        for {
          h <- eh.validated(source)
          t <- et.extract(source, fields.tail)
        } yield h :: t
    }
}

class IsoDecomposer[T, F <: HList, L <: HList](fields: F, iso: Iso[T, L], decomposer: DecomposerAux[F, L])
  extends Decomposer[T] {
    def decompose(t: T) = decomposer.decompose(fields, iso.to(t))
  }

class IsoExtractor[T, F <: HList, L <: HList](fields: F, iso: Iso[T, L], extractor: ExtractorAux[F, L])
  extends Extractor[T] with ValidatedExtraction[T] {
    override def validated(source: JValue) =
      for {
        l <- extractor.extract(source, fields)
      } yield iso.from(l)
  }

class MkDecomposer[T] {
  def apply[F <: HList, L <: HList](fields: F)(implicit iso: Iso[T, L], decomposer: DecomposerAux[F, L]) =
    new IsoDecomposer(fields, iso, decomposer) 
}

class MkExtractor[T] {
  def apply[F <: HList, L <: HList](fields: F)(implicit iso: Iso[T, L], extractor: ExtractorAux[F, L]) = 
    new IsoExtractor(fields, iso, extractor) 
}

class MkSerialization[T] {
  def apply[F <: HList, L <: HList](fields: F)
    (implicit iso: Iso[T, L], decomposer: DecomposerAux[F, L], extractor: ExtractorAux[F, L]) =
      (new IsoDecomposer(fields, iso, decomposer), new IsoExtractor(fields, iso, extractor))
}
