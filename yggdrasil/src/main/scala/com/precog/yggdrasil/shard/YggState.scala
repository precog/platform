package com.precog.yggdrasil
package shard

import com.precog.common._
import com.precog.yggdrasil.util.IOUtils

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

  def newRandomDir(parent: File): File = {
    val newDir = File.createTempFile("col", "", parent)
    newDir.delete
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

  val metadataIO = (descriptor: ProjectionDescriptor, metadata: ColumnMetadata) => {
    descriptorLocator(descriptor).map( d => new File(d, metadataName) ).flatMap {
      f => 
        val metadataSeq = descriptor.columns map { col => metadata(col) }
        IOUtils.safeWriteToFile(pretty(render(metadataSeq.toList.map( _.values.toSet ).serialize)), f)
    }.map(_ => ())
  }
}

object YggState extends Logging {
  val descriptorName = "projection_descriptor.json"
  val metadataName = "projection_metadata.json"
  val checkpointName = "checkpoints.json"

  def restore(dataDir: File): IO[Validation[Error, YggState]] = {
    loadDescriptors(dataDir) flatMap { desc => loadMetadata(desc) map { _.map { meta => (desc, meta) } } } flatMap { tv => tv match {
      case Success((d,m)) => IO { Success(new YggState(dataDir, d, m)) }
      case Failure(e) => IO { Failure(e) }
    }}
  }

  def loadDescriptors(baseDir: File): IO[Map[ProjectionDescriptor, File]] = {
    def loadMap(baseDir: File) = 
      IOUtils.walkSubdirs(baseDir) flatMap { 
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

  def loadMetadata(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, Map[ProjectionDescriptor, ColumnMetadata]]] = {
    def readAll(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, Seq[(ProjectionDescriptor, ColumnMetadata)]]] = {
      val validatedEntries = descriptors.toList.map{ 
        case (d, f) => readSingle(f) map { _.map {
          case metadataSeq => (d, Map((d.columns zip metadataSeq): _*))
        }}
      }.sequence[IO, Validation[Error, (ProjectionDescriptor, ColumnMetadata)]]

      validatedEntries.map(flattenValidations)
    }


    def readSingle(dir: File): IO[Validation[Error, Seq[MetadataMap]]] = {
      import JsonParser._
      val metadataFile = new File(dir, "projection_metadata.json")
      IOUtils.readFileToString(metadataFile).map { _ match {
        case None    => Success(List(Map[MetadataType, Metadata]()))
        case Some(c) => {
          val validatedTuples = parse(c).validated[List[Set[Metadata]]]
          validatedTuples.map( _.map( Metadata.toTypedMap _ ))
        }
      }}
    }

    readAll(descriptors).map { _.map { Map(_: _*) } }
  }

  def flattenValidations[A](l: Seq[Validation[Error,A]]): Validation[Error, Seq[A]] = {
    l.foldLeft[Validation[Error, List[A]]]( Success(List()) ) { (acc, el) => (acc, el) match {
      case (Success(ms), Success(m)) => Success(ms :+ m)
      case (Failure(e1), Failure(e2)) => Failure(e1 |+| e2)
      case (_          , Failure(e)) => Failure(e)
    }}
  }

  class DBIO(dataDir: File, descriptorLocations: Map[ProjectionDescriptor, File]) { 
  }
}

// vim: set ts=4 sw=4 et:
