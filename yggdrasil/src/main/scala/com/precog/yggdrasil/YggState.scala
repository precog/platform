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

trait ProjectionDescriptorStorage {
  def storageLocation(descriptor: ProjectionDescriptor): IO[File]
  def saveDescriptor(descriptor: ProjectionDescriptor): IO[Validation[Throwable, File]]
}

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

  object descriptorStorage extends ProjectionDescriptorStorage {
    def storageLocation(descriptor: ProjectionDescriptor) = IO {
      descriptorState.get(descriptor) match {
        case Some(x) => x
        case None    => {
          val newDir = newDescriptorDir(descriptor, dataDir)
          descriptorState += (descriptor -> newDir)
          newDir
        }
      }
    }

    def saveDescriptor(descriptor: ProjectionDescriptor) = {
      for {
        f <- storageLocation(descriptor).map( d => new File(d, descriptorName))
        v <- IOUtils.safeWriteToFile(pretty(render(descriptor.serialize)), f)
      } yield {
        v map (_ => f)
      }
    }
  }
}

object YggState extends Logging {
  val descriptorName = "projection_descriptor.json"
  val checkpointName = "checkpoints.json"

  def restore(dataDir: File): IO[Validation[Error, YggState]] = {
    for {
      desc <- loadDescriptors(dataDir) 
      metav <- loadMetadata(desc)
    } yield {
      metav map { meta => YggState(dataDir, desc, meta) }
    }
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

    IO { 
      val start = System.nanoTime
      val result = walk(baseDir) 
      val time = (System.nanoTime - start) / 1000000000.0
      logger.debug("Walked data dir %s found %d columns in %.02f".format(baseDir.toString, result.size, time))
      result
    }
  }

  def loadDescriptors(baseDir: File): IO[Map[ProjectionDescriptor, File]] = {
    def loadMap(baseDir: File) = {
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
      loadSingleMetadata(desc, metadataStorage).map{ _.map { (desc, _) } }
    }(collection.breakOut)
    
    dms.sequence[IO, ValProjTuple].map{ flattenValidations }.map{ _.map { Map(_: _*) } }
  }

  def loadSingleMetadata(descriptor: ProjectionDescriptor, metadataStorage: MetadataStorage): IO[Validation[Error, ColumnMetadata]] = {
    // note this is not complete as it doesn't restore the metadata state to match the leveldb columns
    metadataStorage.currentMetadata(descriptor).map { _.map { _.metadata } }
  }

  def flattenValidations[A](l: Seq[Validation[Error,A]]): Validation[Error, Seq[A]] = {
    l.partition { _.isSuccess } match {
      case (ss, es) if es.isEmpty => Success(ss.map { case Success(r) => r})
      case (_, es)                => Failure(es.map{ case Failure(e) => e}.reduce{ _ |+| _})
    }
  }
}

// vim: set ts=4 sw=4 et:
