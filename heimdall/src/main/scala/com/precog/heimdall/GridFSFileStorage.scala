package com.precog.heimdall

import com.precog.common._
import com.precog.common.jobs._

import blueeyes.core.http.{ MimeType, MimeTypes }

import com.mongodb.DB
import com.mongodb.gridfs._

import java.io.InputStream

import scalaz._

object GridFSFileStorage {
  val MaxChunkSize = 10 * 1024 * 1024

  def apply[M[+_]](db: DB)(implicit M0: Monad[M]) = new GridFSFileStorage[M] {
    val M = M0
    val gridFS = new GridFS(db)
  }
}

/**
 * A `FileStorage` implementation that uses Mongo's GridFS to store and
 * retrieve files.
 */
trait GridFSFileStorage[M[+_]] {
  import GridFSFileStorage._
  import scalaz.syntax.monad._

  implicit def M: Monad[M]

  def gridFS: GridFS

  def exists(file: String): M[Boolean] = M.point {
    gridFS.findOne(file) == null
  }

  def save(file: String, data: FileData[M]): M[Unit] = M.point {
    val inFile = gridFS.createFile()
    inFile.setFilename(file)
    data.mimeType foreach { contentType =>
      inFile.setContentType(contentType.toString)
    }
    inFile.getOutputStream()
  } flatMap { out =>
    def save(data: StreamT[M, Array[Byte]]): M[Unit] = data.uncons flatMap {
      case Some((bytes, tail)) => 
        out.write(bytes)
        save(tail)

      case None =>
        M.point { out.close() }
    }

    save(data.data)
  }

  def load(filename: String): M[Option[FileData[M]]] = M.point {
    Option(gridFS.findOne(filename)) map { file =>
      val idealChunkSize = file.getChunkSize
      val chunkSize = if (idealChunkSize > MaxChunkSize) MaxChunkSize else idealChunkSize.toInt
      val mimeType = Option(file.getContentType) flatMap { ct =>
        MimeTypes.parseMimeTypes(ct).headOption
      }
      val in0 = file.getInputStream()

      FileData(mimeType, StreamT.unfoldM[M, Array[Byte], InputStream](in0) { in =>
        M.point {
          val buffer = new Array[Byte](chunkSize)
          val len = in.read(buffer)
          if (len < 0) {
            None
          } else if (len < buffer.length) {
            Some((java.util.Arrays.copyOf(buffer, len), in))
          } else {
            Some((buffer, in))
          }
        }
      })
    }
  }

  def remove(file: String): M[Unit] = M.point(gridFS.remove(file))
}

