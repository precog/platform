package com.precog.yggdrasil

import com.precog.util._
import com.precog.common.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import java.io.File

import scalaz._
import scalaz.Scalaz._
import scalaz.effect._
import scalaz.iteratee._

trait ProjectionFactory {
  type Dataset

  def projection(descriptor: ProjectionDescriptor): ValidationNEL[Throwable, Projection[Dataset]]
}

trait Projection[Dataset] {
  def descriptor: ProjectionDescriptor

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Projection[Dataset]

  def getAllPairs(expiresAt: Long) : Dataset

  def close(): Unit
}
