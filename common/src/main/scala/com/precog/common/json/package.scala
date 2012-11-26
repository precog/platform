package com.precog.common

package object json {
  def extractor[T] = new MkExtractor[T]
  def decomposer[T] = new MkDecomposer[T]
  def serialization[T] = new MkSerialization[T]
}
