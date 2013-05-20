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
import blueeyes.json.serialization.JodaSerializationImplicits._

import com.precog.common.serialization._
import com.precog.util.{FileLock, IOUtils, PrecogUnit}

import com.weiglewilczek.slf4s.Logging

import java.io._
import java.util.UUID
import org.joda.time.Instant

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.syntax.applicative._
import scalaz.syntax.effect.id._
import scalaz.syntax.std.boolean._

import shapeless._

object VersionLog {
  final val lockName = "versionLog"
  final val logName = "versionLog"
  final val completedLogName = "completedLog"
  final val currentVersionFilename = "HEAD"

  final val unsetSentinel = "unset"

  final def hasCurrent(baseDir: File): IO[Boolean] = {
    val currentFile = new File(baseDir, currentVersionFilename)

    IO(currentFile.exists) flatMap { exists =>
      if (exists) {
        IOUtils.readFileToString(currentFile) map { _ != unsetSentinel }
      } else {
        IO(false)
      }
    }
  }

  class LogFiles(val baseDir: File) {
    val headFile = new File(baseDir, currentVersionFilename)
    val logFile = new File(baseDir, logName)
    val completedFile = new File(baseDir, completedLogName)
  }

  def open(baseDir: File): IO[Validation[Error, VersionLog]] = IO {
    if (!baseDir.isDirectory) {
      if (!baseDir.mkdirs) throw new IllegalStateException(baseDir + " cannot be created as a directory.")
    } 

    val logFiles = new LogFiles(baseDir)
    import logFiles._

    // Read in the list of versions as well as the current version
    val currentVersion: Validation[Error, Option[VersionEntry]] = if (headFile.exists) {
      for {
        jv <- JParser.parseFromFile(headFile).leftMap(Error.thrown)
        version <- jv match {
          case JString(`unsetSentinel`) => Success(None)
          case other => other.validated[VersionEntry].map(Some(_))
        }
      } yield version
    } else {
      Success(None)
    }

    val allVersions: Validation[Error, List[VersionEntry]] = if (logFile.exists) {
      for {
        jvs <- JParser.parseManyFromFile(logFile).leftMap(Error.thrown)
        versions <- jvs.toList.traverse[({ type λ[α] = Validation[Error, α] })#λ, VersionEntry](_.validated[VersionEntry])
      } yield versions
    } else {
      Success(Nil)
    }

    val completedVersions: Validation[Error, Set[UUID]] = if (completedFile.exists) {
      for {
        jvs <- JParser.parseManyFromFile(completedFile).leftMap(Error.thrown)
        versions <- jvs.toList.traverse[({ type λ[α] = Validation[Error, α] })#λ, UUID](_.validated[UUID])
      } yield versions.toSet
    } else {
      Success(Set.empty)
    }

    (currentVersion |@| allVersions |@| completedVersions) { new VersionLog(logFiles, _, _, _) }
  }
}

/**
  * Track path versions. This class is not thread safe
  */
class VersionLog(logFiles: VersionLog.LogFiles, initVersion: Option[VersionEntry], initAllVersions: List[VersionEntry], initCompletedVersions: Set[UUID]) extends Logging {
  import VersionLog._
  import logFiles._

  private[this] val workLock = FileLock(logFiles.baseDir, lockName)

  private[this] var currentVersion: Option[VersionEntry] = initVersion
  private[this] var allVersions: List[VersionEntry] = initAllVersions
  private[this] var completedVersions: Set[UUID] = initCompletedVersions

  def current: Option[VersionEntry] = currentVersion
  def find(version: UUID): Option[VersionEntry] = allVersions.find(_.id == version)
  def isCompleted(version: UUID) = completedVersions.contains(version)

  def close = {
    workLock.release
  }

  def addVersion(entry: VersionEntry): IO[PrecogUnit] = allVersions.find(_ == entry) map { _ =>
    IO(PrecogUnit)
  } getOrElse {
    logger.debug("Adding version entry: " + entry)
    IOUtils.writeToFile(entry.serialize.renderCompact + "\n", logFile, true) map { _ =>
      allVersions = allVersions :+ entry
      PrecogUnit
    }
  }

  def completeVersion(version: UUID): IO[PrecogUnit] = {
    if (allVersions.exists(_.id == version)) {
      !isCompleted(version) whenM {
        logger.debug("Completing version " + version)
        IOUtils.writeToFile(version.serialize.renderCompact + "\n", completedFile)
      } map { _ => PrecogUnit }
    } else {
      IO.throwIO(new IllegalStateException("Cannot make nonexistent version %s current" format version))
    }
  }

  def setHead(newHead: UUID): IO[PrecogUnit] = {
    currentVersion.exists(_.id == newHead) unlessM {
      allVersions.find(_.id == newHead) traverse { entry =>
        logger.debug("Setting HEAD to " + newHead)
        IOUtils.writeToFile(entry.serialize.renderCompact + "\n", headFile) map { _ =>
          currentVersion = Some(entry);
        }
      } flatMap {
        _.isEmpty.whenM(IO.throwIO(new IllegalStateException("Attempt to set head to nonexistent version %s" format newHead)))
      }
    } map { _ => PrecogUnit }
  }

  def clearHead = IOUtils.writeToFile(unsetSentinel, headFile).map { _ =>
    currentVersion = None
  }
}

case class VersionEntry(id: UUID, typeName: String, timestamp: Instant)

object VersionEntry {
  implicit val versionEntryIso = Iso.hlist(VersionEntry.apply _, VersionEntry.unapply _)

  val schemaV1 = "id" :: "typeName" :: "timestamp" :: HNil

  implicit val Decomposer: Decomposer[VersionEntry] = decomposerV(schemaV1, Some("1.0".v))
  implicit val Extractor: Extractor[VersionEntry] = extractorV(schemaV1, Some("1.0".v))
}
