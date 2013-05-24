package com.precog.yggdrasil
package vfs

import ResourceError._
import table.Slice
import metadata.PathStructure

import com.precog.common._
import com.precog.common.accounts.AccountId
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.niflheim._
import com.precog.yggdrasil.actor.IngestData
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.yggdrasil.table.ColumnarTableModule
import com.precog.util._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.{Timeout, Duration}

import blueeyes.bkka.FutureMonad
import blueeyes.core.http.MimeType
import blueeyes.core.http.MimeTypes
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.util.Clock

import java.util.UUID
import java.io.{File, IOException, FileInputStream, FileOutputStream}
import java.nio.CharBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor

import com.weiglewilczek.slf4s.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.annotation.tailrec
import scalaz._
import scalaz.NonEmptyList._
import scalaz.EitherT._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.std.option._
import scalaz.std.tuple._

import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.std.list._
import scalaz.syntax.effect.id._

sealed trait PathActionResponse 
sealed trait ReadResult extends PathActionResponse
sealed trait WriteResult extends PathActionResponse

case class UpdateSuccess(path: Path) extends WriteResult
case class PathOpFailure(path: Path, error: ResourceError) extends ReadResult with WriteResult

trait ActorVFSModule extends VFSModule[Future, Slice] {
  type Projection = NIHDBProjection

  def permissionsFinder: PermissionsFinder[Future]
  def jobManager: JobManager[Future] 
  def resourceBuilder: ResourceBuilder

  case class ReadSuccess(path: Path, resource: Resource) extends ReadResult

  /**
   * Used to access resources. This is needed because opening a NIHDB requires
   * more than just a basedir, but also things like the chef, txLogScheduler, etc.
   * This also goes for blobs, where the metadata log requires the txLogScheduler.
   */
  class ResourceBuilder(
    actorSystem: ActorSystem,
    clock: Clock,
    chef: ActorRef,
    cookThreshold: Int,
    storageTimeout: Timeout,
    txLogSchedulerSize: Int = 20) extends Logging { // default for now, should come from config in the future

    private final val txLogScheduler = new ScheduledThreadPoolExecutor(txLogSchedulerSize,
      new ThreadFactoryBuilder().setNameFormat("HOWL-sched-%03d").build())

    private implicit val futureMonad = new FutureMonad(actorSystem.dispatcher)

    private def ensureDescriptorDir(versionDir: File): IO[File] = IO {
      if (versionDir.isDirectory || versionDir.mkdirs) versionDir
      else throw new IOException("Failed to create directory for projection: %s".format(versionDir))
    }

    // Resource creation/open and discovery
    def createNIHDB(versionDir: File, authorities: Authorities): IO[ResourceError \/ NIHDB] = {
      for {
        nihDir <- ensureDescriptorDir(versionDir) 
        nihdbV <- NIHDB.create(chef, authorities, nihDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
      } yield { 
        nihdbV.disjunction leftMap {
          ResourceError.fromExtractorError("Failed to create NIHDB in %s as %s".format(versionDir.toString, authorities)) 
        }
      }
    }

    def openNIHDB(descriptorDir: File): IO[ResourceError \/ NIHDBResource] = {
      NIHDB.open(chef, descriptorDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem) map {
        _ map { 
          _.disjunction map { NIHDBResource(_) } leftMap {
            ResourceError.fromExtractorError("Failed to open NIHDB from %s".format(descriptorDir.toString)) 
          }
        } getOrElse {
          \/.left(NotFound("No NIHDB projection found in %s".format(descriptorDir)))
        }
      }
    }

    final val blobMetadataFilename = "blob_metadata"

    def isBlob(versionDir: File): Boolean = (new File(versionDir, blobMetadataFilename)).exists

    /**
     * Open the blob for reading in `baseDir`.
     */
    def openBlob(versionDir: File): IO[ResourceError \/ FileBlobResource] = IO {
      //val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
      //val metadata = metadataStore.json.validated[BlobMetadata]
      JParser.parseFromFile(new File(versionDir, blobMetadataFilename)).leftMap(Extractor.Error.thrown).
      flatMap(_.validated[BlobMetadata]).
      disjunction.map { metadata =>
        FileBlobResource(new File(versionDir, "data"), metadata) //(actorSystem.dispatcher)
      } leftMap {
        ResourceError.fromExtractorError("Error reading metadata from versionDir %s".format(versionDir.toString))
      }
    }

    /**
     * Creates a blob from a data stream.
     */
    def createBlob[M[+_]](versionDir: File, mimeType: MimeType, authorities: Authorities, data: StreamT[M, Array[Byte]])(implicit M: Monad[M], IOT: IO ~> M): M[ResourceError \/ FileBlobResource] = {
      def write(out: FileOutputStream, size: Long, stream: StreamT[M, Array[Byte]]): M[ResourceError \/ Long] = {
        stream.uncons.flatMap {
          case Some((bytes, tail)) =>
            try {
              out.write(bytes)
              write(out, size + bytes.length, tail)
            } catch { 
              case (ioe: IOException) =>
                out.close()
                \/.left(IOError(ioe)).point[M]
            }

          case None =>
            out.close()
            \/.right(size).point[M]
        }
      }

      for {
        _ <- IOT { IOUtils.makeDirectory(versionDir) }
        file = (new File(versionDir, "data")) 
        _ = logger.debug("Creating new blob at " + file)
        writeResult <- write(new FileOutputStream(file), 0L, data)
        blobResult <- IOT { 
          writeResult traverse { size =>
            logger.debug("Write complete on " + file)
            val metadata = BlobMetadata(mimeType, size, clock.now(), authorities)
            //val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
            //metadataStore.json = metadata.serialize
            IOUtils.writeToFile(metadata.serialize.renderCompact, new File(versionDir, blobMetadataFilename)) map { _ =>
              FileBlobResource(file, metadata)
            } 
          } 
        }
      } yield blobResult
    }
  }


  case class NIHDBResource(val db: NIHDB) extends ProjectionResource with Logging {
    val mimeType: MimeType = FileContent.XQuirrelData

    def authorities = db.authorities

    def append(batch: NIHDB.Batch): IO[PrecogUnit] = db.insert(Seq(batch))

    def projection(implicit M: Monad[Future]) = NIHDBProjection.wrap(db)

    def recordCount(implicit M: Monad[Future]) = projection.map(_.length)

    def byteStream(matchType: Option[MimeType])(implicit M: Monad[Future]): OptionT[Future, StreamT[Future, Array[Byte]]] = {
      OptionT(
        projection map { p =>
          ColumnarTableModule.byteStream(p.getBlockStream(None), matchType)
        }
      )
    }
  }

  object FileBlobResource {
    val ChunkSize = 100 * 1024

    def IOF(implicit M: Monad[Future]): IO ~> Future = new NaturalTransformation[IO, Future] {
      def apply[A](io: IO[A]) = M.point(io.unsafePerformIO)
    }
  }

  /**
   * A blob of data that has been persisted to disk.
   */
  final case class FileBlobResource(dataFile: File, metadata: BlobMetadata) extends BlobResource {
    import FileContent._
    import FileBlobResource._
    
    val authorities: Authorities = metadata.authorities
    val mimeType: MimeType = metadata.mimeType
    val byteLength = metadata.size

    /** Suck the file into a String */
    def asString(implicit M: Monad[Future]): OptionT[Future, String] = OptionT(M point {
      stringTypes.contains(mimeType).option(IOUtils.readFileToString(dataFile)).sequence.unsafePerformIO
    })

    /** Stream the file off disk. */
    def ioStream: StreamT[IO, Array[Byte]] = {
      @tailrec
      def readChunk(fin: FileInputStream, skip: Long): Option[Array[Byte]] = {
        val remaining = skip - fin.skip(skip)
        if (remaining == 0) {
          val bytes = new Array[Byte](ChunkSize)
          val read = fin.read(bytes)

          if (read < 0) None
          else if (read == bytes.length) Some(bytes)
          else Some(java.util.Arrays.copyOf(bytes, read))
        } else {
          readChunk(fin, remaining)
        }
      }

      StreamT.unfoldM[IO, Array[Byte], Long](0L) { offset =>
        IO(new FileInputStream(dataFile)).bracket(f => IO(f.close())) { in =>
          IO(readChunk(in, offset) map { bytes =>
            (bytes, offset + bytes.length)
          })
        }
      }
    }

    def byteStream(mimeType: Option[MimeType])(implicit M: Monad[Future]): OptionT[Future, StreamT[Future, Array[Byte]]] = {
      OptionT(M.point((mimeType.forall(_ == metadata.mimeType)).option(ioStream.trans(IOF))))
    }

    override def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = blobResource(this)
  }

  class ActorVFS(projectionsActor: ActorRef, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS {
    def toJsonElements(slice: Slice) = slice.toJsonElements

    def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit] = {
      IO { projectionsActor ! IngestData(data) }
    }

    def writeAllSync(data: Seq[(Long, EventMessage)]): EitherT[Future, ResourceError, PrecogUnit] = EitherT {
      implicit val timeout = sliceIngestTimeout
      for {
        // it's necessary to group by path then traverse since each path will respond to ingest independently.
        // -- a bit of a leak of implementation detail, but that's the actor model for you.
        allResults <- (data groupBy { case (offset, msg) => msg.path }).toStream traverse { case (path, subset) => 
          (projectionsActor ? IngestData(subset)).mapTo[WriteResult] 
        }
      } yield {
        val errors: List[ResourceError] = allResults.toList collect { case PathOpFailure(_, error) => error }
        errors.toNel.map(ResourceError.all).toLeftDisjunction(PrecogUnit)
      }
    }

    def readResource(path: Path, version: Version): EitherT[Future, ResourceError, Resource] = {
      implicit val t = projectionReadTimeout
      EitherT {
        (projectionsActor ? Read(path, version)).mapTo[ReadResult] map {
          case ReadSuccess(_, resource) => \/.right(resource)
          case PathOpFailure(_, error) => \/.left(error)
        }
      }
    }

    def findDirectChildren(path: Path): Future[Set[Path]] = {
      implicit val t = projectionReadTimeout
      val paths = (projectionsActor ? FindChildren(path)).mapTo[Set[Path]]
      paths map { _ flatMap { _ - path }}
    }

    def pathStructure(path: Path, selector: CPath, version: Version): EitherT[Future, ResourceError, PathStructure] = {
      readProjection(path, version) flatMap { projection => 
        right {
          for (children <- projection.structure) yield {
            PathStructure(projection.reduce(Reductions.count, selector), children.map(_.selector))
          }
        }
      }
    }

    def currentVersion(path: Path) = {
      implicit val t = projectionReadTimeout
      (projectionsActor ? CurrentVersion(path)).mapTo[Option[VersionEntry]]
    }
  }

  case class IngestBundle(data: Seq[(Long, EventMessage)], perms: Map[APIKey, Set[Permission]])

  class PathRoutingActor(baseDir: File, shutdownTimeout: Duration, clock: Clock) extends Actor with Logging {
    private implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)

    private[this] var pathActors = Map.empty[Path, ActorRef]

    override def postStop = {
      logger.info("Shutdown of path actors complete")
    }

    private[this] def targetActor(path: Path): IO[ActorRef] = {
      pathActors.get(path).map(IO(_)) getOrElse {
        val pathDir = VFSPathUtils.pathDir(baseDir, path)

        for {
          _ <- IOUtils.makeDirectory(pathDir) 
          _ = logger.debug("Created new path dir for %s : %s".format(path, pathDir))
          vlog <- VersionLog.open(pathDir) 
          actorV <- vlog traverse { versionLog =>
            logger.debug("Creating new PathManagerActor for " + path)
            context.actorOf(Props(new PathManagerActor(path, VFSPathUtils.versionsSubdir(pathDir), versionLog, shutdownTimeout, clock))) tap { newActor =>
              IO { pathActors += (path -> newActor) }
            }
          }
        } yield {
          actorV valueOr {
            case Extractor.Thrown(t) => throw t
            case error => throw new Exception(error.message)
          }
        }
      }
    }

    def receive = {
      case FindChildren(path) =>
        VFSPathUtils.findChildren(baseDir, path) map { sender ! _ } unsafePerformIO

      case op: PathOp =>
        val requestor = sender
        val io = targetActor(op.path) map { _.tell(op, requestor) } except {
          case t: Throwable =>
            logger.error("Error obtaining path actor for " + op.path, t)
            IO { requestor ! PathOpFailure(op.path, IOError(t)) }
        } 
        
        io.unsafePerformIO

      case IngestData(messages) =>
        logger.debug("Received %d messages for ingest".format(messages.size))
        val requestor = sender
        val groupedAndPermissioned = messages.groupBy({ case (_, event) => event.path }).toStream traverse {
          case (path, pathMessages) =>
            targetActor(path) map { pathActor =>
              pathMessages.map(_._2.apiKey).distinct.toStream traverse { apiKey =>
                permissionsFinder.writePermissions(apiKey, path, clock.instant()) map { perms =>
                  apiKey -> perms.toSet[Permission]
                }
              } map { allPerms =>
                val (totalArchives, totalEvents, totalStoreFiles) = pathMessages.foldLeft((0, 0, 0)) {
                  case ((archived, events, storeFiles), (_, IngestMessage(_, _, _, data, _, _, _))) => (archived, events + data.size, storeFiles)
                  case ((archived, events, storeFiles), (_, am: ArchiveMessage)) => (archived + 1, events, storeFiles)
                  case ((archived, events, storeFiles), (_, sf: StoreFileMessage)) => (archived, events, storeFiles + 1)
                }
                logger.debug("Sending %d archives, %d storeFiles, and %d events to %s".format(totalArchives, totalStoreFiles, totalEvents, path))
                pathActor.tell(IngestBundle(pathMessages, allPerms.toMap), requestor)
              }
            } except {
              case t: Throwable => IO(logger.error("Failure during version log open on " + path, t))
            }
        }

        groupedAndPermissioned.unsafePerformIO
    }
  }

  /**
    * An actor that manages resources under a given path. The baseDir is the version
    * subdir for the path.
    */
  final class PathManagerActor(path: Path, baseDir: File, versionLog: VersionLog, shutdownTimeout: Duration, clock: Clock) extends Actor with Logging {

    private[this] implicit def executor: ExecutionContext = context.dispatcher
    private[this] implicit val futureM = new FutureMonad(executor)

    // Keeps track of the resources for a given version/authority pair
    // TODO: make this an LRU cache
    private[this] var versions = Map[UUID, Resource]()

    override def postStop = {
      val closeAll = versions.values.toStream traverse {
        case NIHDBResource(db) => db.close(context.system)
        case _ => Promise successful PrecogUnit
      }
      
      Await.result(closeAll, shutdownTimeout)
      versionLog.close
      logger.info("Shutdown of path actor %s complete".format(path))
    }

    private def versionDir(version: UUID) = new File(baseDir, version.toString)

    private def canCreate(path: Path, permissions: Set[Permission], authorities: Authorities): Boolean = {
      logger.trace("Checking write permission for " + path + " as " + authorities + " among " + permissions)
      PermissionsFinder.canWriteAs(permissions collect { case perm @ WritePermission(p, _) if p.isEqualOrParentOf(path) => perm }, authorities)
    }

    private def promoteVersion(version: UUID): IO[PrecogUnit] = {
      // we only promote if the requested version is in progress
      if (versionLog.isCompleted(version)) {
        IO(PrecogUnit)
      } else {
        versionLog.completeVersion(version)
      }
    }

    private def openResource(version: UUID): EitherT[IO, ResourceError, Resource] = {
      versions.get(version) map { r =>
        logger.debug("Located existing resource for " + version)
        right(IO(r))
      } getOrElse {
        logger.debug("Opening new resource for " + version)
        versionLog.find(version) map {
          case VersionEntry(v, _, _) =>
            val dir = versionDir(v)
            val openf = if (NIHDB.hasProjection(dir)) { resourceBuilder.openNIHDB _ }
                        else { resourceBuilder.openBlob _ }

            for {
              resource <- EitherT {
                openf(dir) flatMap {
                  _ tap { resourceV =>
                    IO(resourceV foreach { r => versions += (version -> r) })
                  }
                }
              }
            } yield resource 
        } getOrElse {
          left(IO(NotFound("No version found to exist for resource %s.".format(path.path))))
        }
      }
    }

    private def openNIHDB(version: UUID): EitherT[IO, ResourceError, NIHDBResource] = {
      openResource(version) flatMap {
        case nr: NIHDBResource => right(IO(nr))
        case other => left(IO(NotFound("Located resource on %s is a BLOB, not a projection" format path.path)))
      }
    }

    private def performCreate(apiKey: APIKey, data: PathData, version: UUID, writeAs: Authorities, complete: Boolean): IO[PathActionResponse] = {
      implicit val ioId = NaturalTransformation.refl[IO]
      for {
        _ <- versionLog.addVersion(VersionEntry(version, data.typeName, clock.instant()))
        created <- data match {
          case BlobData(bytes, mimeType) =>
            resourceBuilder.createBlob[IO](versionDir(version), mimeType, writeAs, bytes :: StreamT.empty[IO, Array[Byte]])

          case NIHDBData(data) =>
            resourceBuilder.createNIHDB(versionDir(version), writeAs) flatMap {
              _ traverse { nihdb => 
                nihdb tap { _.insert(data) } map { NIHDBResource(_) }
              }
            }
        }
        _ <- created traverse { resource =>
          for {
            _ <- IO { versions += (version -> resource) }
            _ <- complete.whenM(versionLog.completeVersion(version) >> versionLog.setHead(version))
          } yield PrecogUnit
        }
      } yield {
        created.fold(
          error => PathOpFailure(path, error),
          _ => UpdateSuccess(path)
        )
      }
    }

    private def maybeCompleteJob(msg: EventMessage, terminal: Boolean, response: PathActionResponse) = {
      //TODO: Add job progress updates
      (response == UpdateSuccess(msg.path) && terminal).option(msg.jobId).join traverse { jobManager.finish(_, clock.now()) } map { _ => response }
    }

    def processEventMessages(msgs: Stream[(Long, EventMessage)], permissions: Map[APIKey, Set[Permission]], requestor: ActorRef): IO[PrecogUnit] = {
      logger.debug("About to persist %d messages; replying to %s".format(msgs.size, requestor.toString))

      def persistNIHDB(createIfAbsent: Boolean, offset: Long, msg: IngestMessage, streamId: UUID, terminal: Boolean): IO[PrecogUnit] = {
        def batch(msg: IngestMessage) = NIHDB.Batch(offset, msg.data.map(_.value))

        openNIHDB(streamId).fold[IO[PrecogUnit]](
          {
            case NotFound(_) =>
              if (createIfAbsent) {
                logger.trace("Creating new nihdb database for streamId " + streamId)
                performCreate(msg.apiKey, NIHDBData(List(batch(msg))), streamId, msg.writeAs, terminal) map { response =>
                  maybeCompleteJob(msg, terminal, response) pipeTo requestor
                  PrecogUnit
                }
              } else {
                //TODO: update job
                logger.warn("Cannot overwrite existing database for " + streamId)
                IO(requestor ! PathOpFailure(path, IllegalWriteRequestError("Cannot overwrite existing resource. %s not applied.".format(msg.toString))))
              }

            case other =>
              IO(requestor ! PathOpFailure(path, other))
          },
          resource => for {
            _ <- resource.append(batch(msg))
            // FIXME: completeVersion and setHead should be one op
            _ <- terminal.whenM(versionLog.completeVersion(streamId) >> versionLog.setHead(streamId))
          } yield {
            logger.trace("Sent insert message for " + msg + " to nihdb")
            // FIXME: We aren't actually guaranteed success here because NIHDB might do something screwy.
            maybeCompleteJob(msg, terminal, UpdateSuccess(msg.path)) pipeTo requestor
            PrecogUnit
          }
        ).join
      }

      def persistFile(createIfAbsent: Boolean, offset: Long, msg: StoreFileMessage, streamId: UUID, terminal: Boolean): IO[PrecogUnit] = {
        logger.debug("Persisting file on %s for offset %d".format(path, offset))
        // TODO: I think the semantics here of createIfAbsent aren't
        // quite right. If we're in a replay we don't want to return
        // errors if we're already complete
        if (createIfAbsent) {
          performCreate(msg.apiKey, BlobData(msg.content.data, msg.content.mimeType), streamId, msg.writeAs, terminal) map { response =>
            maybeCompleteJob(msg, terminal, response) pipeTo requestor
            PrecogUnit
          }
        } else {
          //TODO: update job
          IO(requestor ! PathOpFailure(path, IllegalWriteRequestError("Cannot overwrite existing resource. %s not applied.".format(msg.toString))))
        }
      }

      msgs traverse {
        case (offset, msg @ IngestMessage(apiKey, path, _, _, _, _, streamRef)) =>
          streamRef match {
            case StreamRef.Create(streamId, terminal) =>
              logger.trace("Received create for %s stream %s: current: %b, complete: %b".format(path.path, streamId, versionLog.current.isEmpty, versionLog.isCompleted(streamId)))
              persistNIHDB(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Replace(streamId, terminal) =>
              logger.trace("Received replace for %s stream %s: complete: %b".format(path.path, streamId, versionLog.isCompleted(streamId)))
              persistNIHDB(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Append =>
              logger.trace("Received append for %s".format(path.path))
              val streamId = versionLog.current.map(_.id).getOrElse(UUID.randomUUID())
              for {
                _ <- persistNIHDB(canCreate(msg.path, permissions(apiKey), msg.writeAs), offset, msg, streamId, false)
                _ <- versionLog.completeVersion(streamId) >> versionLog.setHead(streamId)
              } yield PrecogUnit
          }

        case (offset, msg @ StoreFileMessage(_, path, _, _, _, _, _, streamRef)) =>
          streamRef match {
            case StreamRef.Create(streamId, terminal) =>
              if (! terminal) {
                logger.warn("Non-terminal BLOB for %s will not currently behave correctly!".format(path))
              }
              persistFile(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Replace(streamId, terminal) =>
              if (! terminal) {
                logger.warn("Non-terminal BLOB for %s will not currently behave correctly!".format(path))
              }
              persistFile(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Append =>
              IO(requestor ! PathOpFailure(path, IllegalWriteRequestError("Append is not yet supported for binary files.")))
          }

        case (offset, ArchiveMessage(apiKey, path, jobId, eventId, timestamp)) =>
          versionLog.clearHead >> IO(requestor ! UpdateSuccess(path))
      } map {
        _ => PrecogUnit
      }
    }

    def versionOpt(version: Version) = version match {
      case Version.Archived(id) => Some(id)
      case Version.Current => versionLog.current.map(_.id)
    }

    def receive = {
      case IngestBundle(messages, permissions) =>
        logger.debug("Received ingest request for %d messages.".format(messages.size))
        processEventMessages(messages.toStream, permissions, sender).unsafePerformIO

      case msg @ Read(_, version) =>
        logger.debug("Received Read request " + msg)

        val requestor = sender
        val io: IO[ReadResult] = versionOpt(version) map { version =>
          openResource(version).fold(
            error => PathOpFailure(path, error),
            resource => ReadSuccess(path, resource) 
          )
        } getOrElse {
          IO(PathOpFailure(path, Corrupt("Unable to determine any resource version for path %s".format(path.path))))
        } 
        
        io.map(requestor ! _).unsafePerformIO

      case CurrentVersion(_) =>
        sender ! versionLog.current
    }
  }
}
