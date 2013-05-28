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

import akka.dispatch.Future

import com.precog.common.Path
import com.precog.common.security.{APIKey, PermissionsFinder}
import com.precog.niflheim.NIHDBActor

import com.weiglewilczek.slf4s.Logging

import java.io.{File, FileFilter}

import org.apache.commons.io.filefilter.FileFilterUtils

import scalaz.effect.IO
import scalaz.std.list._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse._

object VFSPathUtils extends Logging {
  // Methods for dealing with path escapes, lookup, enumeration
  private final val disallowedPathComponents = Set(".", "..")

  // For a given path directory, this subdir holds the full set of version dirs
  private final val versionsSubdir = "pathVersions"

  private[yggdrasil] final val escapeSuffix = "_byUser"

  private final val pathFileFilter: FileFilter = {
    import FileFilterUtils.{notFileFilter => not, _}
    not(nameFileFilter(versionsSubdir))
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

  def findChildren(baseDir: File, path: Path, apiKey: APIKey, permissionsFinder: PermissionsFinder[Future]): Future[Set[Path]] = {
    for {
      allowedPaths <- permissionsFinder.findBrowsableChildren(apiKey, path)
    } yield {
      val pathRoot = pathDir(baseDir, path)

      logger.debug("Checking for children of path %s in dir %s among %s".format(path, pathRoot, allowedPaths))
      Option(pathRoot.listFiles(pathFileFilter)).map { files =>
        logger.debug("Filtering children %s in path %s".format(files.mkString("[", ", ", "]"), path))

        (files.toList.flatMap { f =>
          // First pass filtering checks perms against the path
          val fPath = unescapePath(path / Path(f.getName))
          allowedPaths.exists(_.isEqualOrParentOf(fPath)).option(fPath -> f)
        } traverse {
          // Second pass filtering ensures that the child has current data somewhere under it
          case (path, dir) =>
            hasCurrentData(dir) map {
              path -> _
            }
        }).map {
          _.collect {
            case (path, true) => path
          }.toSet
        }.unsafePerformIO
      } getOrElse {
        logger.debug("Path dir %s for path %s is not a directory!".format(pathRoot, path))
        Set.empty
      }
    }
  }

  def hasCurrentData(dir: File): IO[Boolean] = {
    IO(dir.isDirectory) flatMap { isDir =>
      if (isDir) {
        // hasCurrent relies on atomic reads of disk blocks and the fact that a "current version" fits entirely within a block
        VersionLog.hasCurrent(dir) flatMap { hasCurrent =>
          if (hasCurrent) {
            IO(true)
          } else {
            // Recurse on children
            IO(Option(dir.listFiles(pathFileFilter))) flatMap { optFiles =>
              optFiles map { files =>
                files.filter(_.isDirectory).foldLeft(IO(false)) {
                  case (lastCheck, childDir) => lastCheck flatMap {
                    case true  => IO(true) // We already found a child with data, so no need to recurse
                    case false => hasCurrentData(childDir)
                  }
                }
              } getOrElse IO(false)
            }
          }
        }
      } else {
        IO(false)
      }
    }
  }
}
