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
package com.precog.yggdrasil
package vfs

import akka.dispatch.Future

import com.precog.common.Path
import com.precog.common.security.{APIKey, PermissionsFinder}
import com.precog.niflheim.NIHDBActor
import com.precog.yggdrasil.metadata._
import ResourceError._

import com.weiglewilczek.slf4s.Logging

import java.io.{File, FileFilter}

import org.apache.commons.io.filefilter.FileFilterUtils

import scalaz._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse._

object VFSPathUtils extends Logging {
  // Methods for dealing with path escapes, lookup, enumeration
  private final val disallowedPathComponents = Set(".", "..")

  // For a given path directory, this subdir holds the full set of version dirs
  private final val versionsSubdir = "pathVersions"

  // This is the previous "hidden" projection dir. Needed to filter in case of symlinking
  private final val perAuthProjectionsDir = "perAuthProjections"

  private[yggdrasil] final val escapeSuffix = "_byUser"

  private final val pathFileFilter: FileFilter = {
    import FileFilterUtils.{notFileFilter => not, _}
    and(not(nameFileFilter(versionsSubdir)), not(nameFileFilter(perAuthProjectionsDir)))
  }

  def escapePath(path: Path, toEscape: Set[String]) =
    Path(path.elements.map {
      case needsEscape if toEscape.contains(needsEscape) || needsEscape.endsWith(escapeSuffix) =>
        needsEscape + escapeSuffix
      case fine => fine
    }.toList)

  def unescapePath(path: Path) =
    Path(path.elements.map {
      case escaped if escaped.endsWith(escapeSuffix) =>
        escaped.substring(0, escaped.length - escapeSuffix.length)
      case fine => fine
    }.toList)

  /**
    * Computes the stable path for a given vfs path relative to the given base dir. Version subdirs
    * for the given path will reside under this directory
    */
  def pathDir(baseDir: File, path: Path): File = {
    // The path component maps directly to the FS
    val prefix = escapePath(path, Set(versionsSubdir)).elements.filterNot(disallowedPathComponents)
    new File(baseDir, prefix.mkString(File.separator))
  }

  def versionsSubdir(pathDir: File): File = new File(pathDir, versionsSubdir)

  def findChildren(baseDir: File, path: Path): IO[Set[PathMetadata]] = {
    val pathRoot = pathDir(baseDir, path)

    logger.debug("Checking for children of path %s in dir %s".format(path, pathRoot))
    Option(pathRoot.listFiles(pathFileFilter)) map { files =>
      logger.debug("Filtering children %s in path %s".format(files.mkString("[", ", ", "]"), path))
      val childMetadata = files.toList traverse { f => 
        val childPath = unescapePath(path / Path(f.getName))
        currentPathMetadata(baseDir, childPath).fold[Option[PathMetadata]](
          {
            case NotFound(message) =>
              logger.trace("No child data found for %s".format(childPath.path))
              None
            case error => 
              logger.error("Encountered corruption or error searching child paths: %s".format(error.messages.list.mkString("; ")))
              None
          },
          pathMetadata => Some(pathMetadata)
        ) 
      }

      childMetadata.map(_.flatten.toSet): IO[Set[PathMetadata]]
    } getOrElse {
      logger.debug("Path dir %s for path %s is not a directory!".format(pathRoot, path))
      IO(Set.empty)
    }
  }

  def currentPathMetadata(baseDir: File, path: Path): EitherT[IO, ResourceError, PathMetadata] = {
    def containsNonemptyChild(dirs: List[File]): IO[Boolean] = dirs match {
      case f :: xs =>
        val childPath = unescapePath(path / Path(f.getName))
        findChildren(baseDir, childPath) flatMap { children =>
          if (children.nonEmpty) IO(true) else containsNonemptyChild(xs)
        }
    }

    val pathDir0 = pathDir(baseDir, path)
    EitherT {
      IO(pathDir0.isDirectory) flatMap { 
        case true =>
          VersionLog.currentVersionEntry(pathDir0).run flatMap { currentVersionV =>
            currentVersionV.fold[IO[ResourceError \/ PathMetadata]](
              {
                case NotFound(message) =>
                  // Recurse on children to find one that is nonempty
                  containsNonemptyChild(Option(pathDir0.listFiles(pathFileFilter)).toList.flatten) map {
                    case true =>
                      \/.right(PathMetadata(path, PathMetadata.PathOnly))
                    case false =>
                      \/.left(NotFound("All subpaths of %s appear to be empty.".format(path.path)))
                  }

                case otherError =>
                  IO(\/.left(otherError))
              },
              { 
                case VersionEntry(uuid, dataType, timestamp) => 
                  containsNonemptyChild(Option(pathDir0.listFiles(pathFileFilter)).toList.flatten) map {
                    case true => \/.right(PathMetadata(path, PathMetadata.DataDir(dataType.contentType)))
                    case false => \/.right(PathMetadata(path, PathMetadata.DataOnly(dataType.contentType)))
                  }
              }
            )
          }

        case false =>
          IO(\/.left(NotFound("No data found at path %s".format(path.path))))
      }
    }
  }

}
