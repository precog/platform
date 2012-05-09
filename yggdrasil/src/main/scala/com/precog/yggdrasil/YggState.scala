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

import actor._
import metadata._

import com.precog.common._
import com.precog.common.util.IOUtils

import blueeyes.json.Printer._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor
import blueeyes.json.xschema.Extractor._

import java.io._
import java.util.Properties

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.AllInstances._
import scalaz.syntax.biFunctor._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._
import scalaz.effect.IO

case class YggState(
  dataDir: File,
  descriptors: Map[ProjectionDescriptor, File], 
  metadata: Map[ProjectionDescriptor, ColumnMetadata]) {

  private var descriptorState = descriptors 

  import YggState._

  private def dirUUID(): String = {
    val uuid = java.util.UUID.randomUUID.toString.toLowerCase.replace("-", "")
    (1.until(3).map { _*2 }.foldLeft(Vector.empty[String]) {
      case (acc, i) => acc :+ uuid.substring(0,i)
    }.mkString("/", "/", "/")) + uuid
  }

  def newRandomDir(parent: File): File = {
    val newDir = new File(parent, dirUUID)
    newDir.mkdirs
    newDir
  }

  def newDescriptorDir(descriptor: ProjectionDescriptor, parent: File): File = newRandomDir(parent)

  val descriptorLocator = (descriptor: ProjectionDescriptor) => IO {
    descriptorState.get(descriptor) match {
      case Some(x) => x
      case None    => {
        val newDir = newDescriptorDir(descriptor, dataDir)
        descriptorState += (descriptor -> newDir)
        newDir
      }
    }
  }

  val descriptorIO = (descriptor: ProjectionDescriptor) =>
    descriptorLocator(descriptor).map( d => new File(d, descriptorName) ).flatMap {
      f => IOUtils.safeWriteToFile(pretty(render(descriptor.serialize)), f)
    }.map(_ => ())

}

object YggState extends Logging {
  val descriptorName = "projection_descriptor.json"
  val checkpointName = "checkpoints.json"

  def restore(dataDir: File): IO[Validation[Error, YggState]] = {
    loadDescriptors(dataDir) flatMap { desc => loadMetadata(desc) map { _.map { meta => (desc, meta) } } } flatMap { tv => tv match {
      case Success((d,m)) => IO { Success(new YggState(dataDir, d, m)) }
      case Failure(e) => IO { Failure(e) }
    }}
  }

  def walkDirs(baseDir: File): IO[Seq[File]] = {
   
    def containsDescriptor(dir: File) = new File(dir, descriptorName).isFile 

    def walk(baseDir: File): Seq[File] = {
      if(containsDescriptor(baseDir)) {
        Vector(baseDir)
      } else {
        baseDir.listFiles.filter(_.isDirectory).flatMap{ walk(_) }
      }
    }

    IO { walk(baseDir) }
  }

  def loadDescriptors(baseDir: File): IO[Map[ProjectionDescriptor, File]] = {
    def loadMap(baseDir: File) = 
      walkDirs(baseDir) flatMap { 
        _.foldLeft( IO(Map.empty[ProjectionDescriptor, File]) ) { (acc, dir) =>
          logger.debug("loading: " + dir)
          read(dir) flatMap {
            case Success(pd) => acc.map(_ + (pd -> dir))
            case Failure(error) => 
              logger.warn("Failed to restore %s: %s".format(dir, error))
              acc
          }
        }
      }

    def read(baseDir: File): IO[Validation[String, ProjectionDescriptor]] = IO {
      val df = new File(baseDir, "projection_descriptor.json")
      if (!df.exists) Failure("Unable to find serialized projection descriptor in " + baseDir)
      else {
        val reader = new FileReader(df)
        try {
          { (err: Extractor.Error) => err.message } <-: JsonParser.parse(reader).validated[ProjectionDescriptor]
        } finally {
          reader.close
        }
      }
    }

    loadMap(baseDir)
  }

  type ValProjTuple = Validation[Error, (ProjectionDescriptor, ColumnMetadata)]

  def loadMetadata(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, Map[ProjectionDescriptor, ColumnMetadata]]] = {
   
    val metadataStorage = new FilesystemMetadataStorage((desc: ProjectionDescriptor) => IO { descriptors(desc) })
    
    val dms: List[IO[ValProjTuple]] = descriptors.keys.map { desc => 
      loadSingleMetadata(desc, metadataStorage).map{ _.map { (desc, _) }}
    }(collection.breakOut)
    
    dms.sequence[IO, ValProjTuple].map(flattenValidations).map{ _.map { Map(_: _*) } }
  }

  def loadSingleMetadata(descriptor: ProjectionDescriptor, metadataStorage: MetadataStorage): IO[Validation[Error, ColumnMetadata]] = {
    // note this is not complete as it doesn't restore the metadata state to match the leveldb columns
    metadataStorage.currentMetadata(descriptor).map { _.map { _.metadata } }
  }

  def flattenValidations[A](l: Seq[Validation[Error,A]]): Validation[Error, Seq[A]] = {
    l.foldLeft[Validation[Error, List[A]]]( Success(List()) ) { 
      case (Success(ms), Success(m)) => Success(ms :+ m)
      case (Failure(e1), Failure(e2)) => Failure(e1 |+| e2)
      case (_          , Failure(e)) => Failure(e)
    }
  }
}

// vim: set ts=4 sw=4 et:
