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

  def findChildren(baseDir: File, path: Path, apiKey: APIKey, permissionsFinder: PermissionsFinder[Future]): Future[Set[Path]] = {
    for {
      allowedPaths <- permissionsFinder.findBrowsableChildren(apiKey, path)
    } yield {
      val pathRoot = pathDir(baseDir, path)

      logger.debug("Checking for children of path %s in dir %s among %s".format(path, pathRoot, allowedPaths))
      Option(pathRoot.listFiles(pathFileFilter)).map { files =>
        logger.debug("Filtering children %s in path %s".format(files.mkString("[", ", ", "]"), path))
        files.filter(_.isDirectory).map { dir => unescapePath(path / Path(dir.getName)) }.filter { p => allowedPaths.exists(_.isEqualOrParent(p)) }.toSet
      } getOrElse {
        logger.debug("Path dir %s for path %s is not a directory!".format(pathRoot, path))
        Set.empty
      }
    }
  }
}
