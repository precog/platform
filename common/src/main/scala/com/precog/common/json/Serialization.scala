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
