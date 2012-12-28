package com.precog.common

package object json {
  def extractorV[T] = new MkExtractorV[T]
  def decomposerV[T] = new MkDecomposerV[T]
  def serializationV[T] = new MkSerializationV[T]
}
