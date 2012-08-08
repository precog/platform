package com.precog.yggdrasil
package jdbm3

import iterable.IterableDataset
import table._
import com.precog.util.FileOps

import org.joda.time.DateTime

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{Executors,TimeoutException}

import scala.collection.Iterator
import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._

import com.weiglewilczek.slf4s.Logger

trait JDBMProjectionModule extends ProjectionModule {
  val pmLogger = Logger("JDBMProjectionModule")

  type Key = Identities
  class Projection private[JDBMProjectionModule] (baseDir: File, descriptor: ProjectionDescriptor) extends JDBMProjection(baseDir, descriptor)

  trait JDBMProjectionCompanion extends ProjectionCompanion {
    def fileOps: FileOps

    def baseDir(descriptor: ProjectionDescriptor): File

    def open(descriptor: ProjectionDescriptor): IO[Projection] = {
      pmLogger.debug("Opening JDBM projection for " + descriptor)
      val base = baseDir(descriptor)
      val baseDirV: IO[File] = 
        fileOps.exists(base) flatMap { 
          case true  => IO(base)
          case false => fileOps.mkdir(base) map {
                          case true  => base
                          case false => throw new RuntimeException("Could not create database basedir " + base)
                        }
        }

      baseDirV map { (bd: File) => new Projection(bd, descriptor) }
    }

    def close(projection: Projection) = {
      pmLogger.debug("Requesting close on " + projection)
      IO(projection.close())
    }
  }
}
