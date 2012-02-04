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
package shard

import com.precog.common._
import com.precog.yggdrasil.util.IOUtils

import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor
import blueeyes.json.xschema.Extractor._

import java.io._
import java.util.Properties

import com.weiglewilczek.slf4s.Logging
import scala.collection.mutable

import scalaz._
import scalaz.std.AllInstances._
import scalaz.syntax.biFunctor._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._
import scalaz.effect.IO


class ShardConfig(
  val properties: Properties, 
  val baseDir: File, 
  val descriptors: Map[ProjectionDescriptor, File], 
  val metadata: mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]], 
  val checkpoints: mutable.Map[Int, Int])


object ShardConfig extends Logging {
  type MetadataSeq = Seq[mutable.Map[MetadataType, Metadata]]
  
  def fromFile(propsFile: File): IO[Validation[Error, ShardConfig]] = IOUtils.readPropertiesFile { propsFile } flatMap { fromProperties } 
  
  def fromProperties(props: Properties): IO[Validation[Error, ShardConfig]] = {
    val baseDir = extractBaseDir { props }
    loadDescriptors(baseDir) flatMap { desc => loadMetadata(desc) map { _.map { meta => (desc, meta) } } } flatMap { tv => tv match {
      case Success(t) => loadCheckpoints(baseDir) map { _.map( new ShardConfig(props, baseDir, t._1, t._2, _)) }
      case Failure(e) => IO { Failure(e) }
    }}
  }

  def extractBaseDir(props: Properties): File = new File(props.getProperty("precog.storage.root", "."))

  def loadDescriptors(baseDir: File): IO[Map[ProjectionDescriptor, File]] = {
    def loadMap(baseDir: File) = 
      IOUtils.walkSubdirs(baseDir) flatMap { 
        _.foldLeft( IO(Map.empty[ProjectionDescriptor, File]) ) { (acc, dir) =>
          println("loading: " + dir)
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

  def loadMetadata(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, mutable.Map[ProjectionDescriptor, MetadataSeq]]] = {
    def readAll(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, Seq[(ProjectionDescriptor, MetadataSeq)]]] = {
      val validatedEntries = descriptors.toList.map{ case (d, f) => readSingle(f) map { _.map((d, _)) } }.sequence[IO, Validation[Error, (ProjectionDescriptor, MetadataSeq)]]

      validatedEntries.map(flattenValidations)
    }

    def readSingle(dir: File): IO[Validation[Error, MetadataSeq]] = {
      import JsonParser._
      val metadataFile = new File(dir, "projection_metadata.json")
      IOUtils.readFileToString(metadataFile).map { _ match {
        case None    => Success(List(mutable.Map[MetadataType, Metadata]()))
        case Some(c) => {
          val validatedTuples = parse(c).validated[List[List[(MetadataType, Metadata)]]]
          validatedTuples.map( _.map( mutable.Map(_: _*)))
        }
      }}
    }

    readAll(descriptors).map { _.map { mutable.Map(_: _*) } }
  }

  def loadCheckpoints(baseDir: File): IO[Validation[Error, mutable.Map[Int, Int]]] = {
    import JsonParser._
    val checkpointFile = new File(baseDir, "checkpoints.json")
    IOUtils.readFileToString(checkpointFile).map { _ match { 
      case None    => Success(mutable.Map[Int, Int]())
      case Some(c) => parse(c).validated[List[(Int, Int)]].map( mutable.Map(_: _*))
    }}
  }

  def flattenValidations[A](l: Seq[Validation[Error,A]]): Validation[Error, Seq[A]] = {
    l.foldLeft[Validation[Error, List[A]]]( Success(List()) ) { (acc, el) => (acc, el) match {
      case (Success(ms), Success(m)) => Success(ms :+ m)
      case (Failure(e1), Failure(e2)) => Failure(e1 |+| e2)
      case (_          , Failure(e)) => Failure(e)
    }}
  }
}


// vim: set ts=4 sw=4 et:
