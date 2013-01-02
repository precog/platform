package com.precog.heimdall

import scala.collection.JavaConverters._

import com.precog.common._
import com.precog.common.jobs._

import blueeyes.core.http.{ MimeType, MimeTypes }

import com.mongodb.{ Mongo, DB, ServerAddress }
import com.mongodb.gridfs._

import java.io.InputStream

import org.streum.configrity.Configuration

import scalaz._

object GridFSFileStorage {
  val MaxChunkSize = 10 * 1024 * 1024

  private val ServerAndPortPattern = "(.+):(.+)".r

  def apply[M[+_]: Monad](config: Configuration): GridFSFileStorage[M] = {

    // Shamefully ripped off from BlueEyes.

    val servers = config[List[String]]("servers").toList map {
      case ServerAndPortPattern(host, port) => new ServerAddress(host.trim(), port.trim().toInt)
      case server => new ServerAddress(server, ServerAddress.defaultPort())
    }

    val mongo = servers match {
      case x :: Nil => new com.mongodb.Mongo(x)
      case x :: xs  => new com.mongodb.Mongo(servers.asJava)
      case Nil => sys.error("""MongoServers are not configured. Configure the value 'servers'. Format is '["host1:port1", "host2:port2", ...]'""")
    }

    apply(mongo.getDB(config[String]("database")))
  }

  def apply[M[+_]](db: DB)(implicit M0: Monad[M]) = new GridFSFileStorage[M] {
    val M = M0
    val gridFS = new GridFS(db)
  }
}

/**
 * A `FileStorage` implementation that uses Mongo's GridFS to store and
 * retrieve files.
 */
trait GridFSFileStorage[M[+_]] extends FileStorage[M] {
  import GridFSFileStorage._
  import scalaz.syntax.monad._

  implicit def M: Monad[M]

  def gridFS: GridFS

  def exists(file: String): M[Boolean] = M.point {
    gridFS.findOne(file) != null
  }

  def save(file: String, data: FileData[M]): M[Unit] = M.point {
    val inFile = gridFS.createFile(file)
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

