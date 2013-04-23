package com.precog
package yggdrasil
package vfs

case class BlobMetadata(mimeType: MimeType, size: Long, created: DateTime, authorities: Authorities)

object BlobMetadata {
  implicit val iso = Iso.hlist(BlobMetadata.apply _, BlobMetadata.unapply _)

  import IsoSerialization._
  val schema = "mimeType" :: "size" :: "created" :: "authorities" :: HNil
  implicit val decomposer = decomposer[BlobMetadata](schema)
  implicit val extractor = extractor[BlobMetadata](schema)
}

/**
 * A blob of data that has been persisted to disk.
 */
final case class Blob(dataFile: File, metadata: BlobMetadata)(implicit ec: ExecutionContext) extends Resource {
  def authorities: Future[Authorities] = Future(metadata.authorities)
  def mimeType: Future[MimeType] = Future(metadata.mimeType)

  /** Stream the file off disk. */
  def stream: StreamT[IO, Array[Byte]] = {

    @tailrec
    def readChunk(fin: FileInputStream, skip: Int): Option[Array[Byte]] = {
      val remaining = skip - fin.skip(skip)
      if (remaining == 0) {
        val bytes = new Array[Byte](Blob.ChunkSize)
        val read = fin.read(bytes)

        if (read < 0) None
        else if (read == bytes.length) Some(bytes)
        else Some(java.util.Arrays.copyOf(bytes, read)

        } else {
          readChunk(remaining)
        }
      }
    }

    StreamT.unfoldM[IO, Array[Byte], Long](0L) { offset =>
      IO(new FileInputStream(dataFile)).bracket(IO(_.close())) { in =>
        IO(readChunk(in, offset) map { bytes =>
          (bytes, offset + bytes.length)
        })
      }
    }
  }

  def append(data: PathData): Future[PrecogUnit] = data match {
    case _ => Promise.failed(new IllegalArgumentException("Blob append not yet supported"))

    case BlobData(bytes, mimeType, _) => Future {
      if (mimeType != metadata.mimeType) {
        throw new IllegalArgumentException("Attempt to append %s data to a %s blob".format(mimeType, metadata.mimeType))
      }

      val output = new FileOutputStream(dataFile, true)
      output.write(bytes)
      output.close
    }

    case _ => Promise.failed(new IllegalArgumentException("Attempt to insert non-blob data to blob"))
  }

  def close = Promise.successful(PrecogUnit)
}

object Blob {
  val ChunkSize = 100 * 1024
}
