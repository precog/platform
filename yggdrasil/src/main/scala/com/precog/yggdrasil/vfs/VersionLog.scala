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
package com.precog
package yggdrasil
package vfs

import blueeyes.json.{ JParser, JString, JValue }
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._

import com.precog.util.{FileLock, IOUtils, PrecogUnit}

import com.weiglewilczek.slf4s.Logging

import java.io._
import java.util.UUID

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

import shapeless._

object VersionLog {
  final val lockName = "versionLog"
  final val logName = "versionLog"
  final val currentVersionFilename = "HEAD"
}

/**
  * Track path versions. This class is not thread safe
  */
class VersionLog(baseDir: File) extends Logging {
  import VersionLog._

  private[this] val workLock = FileLock(baseDir, lockName)

  private[this] var currentVersion: Option[VersionEntry] = None
  private[this] var allVersions: List[VersionEntry] = Nil

  private[this] val headFile = new File(baseDir, currentVersionFilename)
  private[this] val logFile = new File(baseDir, logName)

  // Read in the list of versions as well as the current version
  if (headFile.exists) {
    currentVersion = (for {
      jv <- JParser.parseFromFile(headFile).leftMap(Error.thrown)
      version <- jv.validated[VersionEntry]
    } yield Some(version)).valueOr { case Thrown(t) => throw t }
  }

  if (logFile.exists) {
    allVersions = (for {
      jvs <- JParser.parseManyFromFile(logFile).leftMap(Error.thrown)
      versions <- jvs.toList.traverse[({ type λ[α] = Validation[Error, α] })#λ, VersionEntry](_.validated[VersionEntry])
    } yield versions).valueOr { error => throw new Exception(error.message) }
  }

  def current = currentVersion
  def find(version: UUID): Option[VersionEntry] = allVersions.find(_.id == version)

  def close = {
    workLock.release
  }

  def addVersion(entry: VersionEntry): IO[PrecogUnit] = {
    IOUtils.writeToFile(entry.serialize.renderCompact + "\n", logFile, true).map { _ =>
      allVersions = allVersions :+ entry
      PrecogUnit
    }
  }

  def setHead(id: UUID): IO[PrecogUnit] = {
    if (currentVersion.exists(_ == id)) {
      IO(PrecogUnit)
    } else {
      allVersions.find(_.id == id).toSuccess(new IllegalStateException("Failed to locate entry to promote: " + id)).map { entry =>
        currentVersion = Some(entry)
        IOUtils.writeToFile(entry.serialize.renderCompact + "\n", headFile)
      }.valueOr(IO.throwIO)
    }
  }
}


case class VersionEntry(id: UUID, typeName: String)

object VersionEntry {
  implicit val versionEntryIso = Iso.hlist(VersionEntry.apply _, VersionEntry.unapply _)

  implicit val uuidDecomposer: Decomposer[UUID] = new Decomposer[UUID] {
    def decompose(u: UUID) = JString(u.toString)
  }

  implicit val uuidExtractor: Extractor[UUID] = new Extractor[UUID] {
    def validated(jv: JValue) = jv.validated[String].map(UUID.fromString)
  }

  val schemaV1 = "id" :: "typeName" :: HNil

  implicit val Decomposer: Decomposer[VersionEntry] = decomposerV(schemaV1, Some("1.0".v))
  implicit val Extractor: Extractor[VersionEntry] = extractorV(schemaV1, Some("1.0".v))
}
