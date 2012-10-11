package com.precog
package pandora

import common.VectorCase
import common.kafka._
import common.security._

import daze._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.metadata._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.test.YId
import muspelheim._

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.util.FilesystemFileOps

import org.specs2.mutable._
  
import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._

import java.io.File

import blueeyes.json._
import JsonAST._

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait PlatformSpecs[M[+_]]
    extends ParseEvalStackSpecs[M] 
    with BlockStoreColumnarTableModule[M] { 

  implicit def M: Monad[M] with Copointed[M]

  class YggConfig extends ParseEvalStackSpecConfig with StandaloneShardSystemConfig with EvaluatorConfig
  object yggConfig  extends YggConfig
}

class TrampolinePlatformSpecs extends PlatformSpecs[YId] 
    with BaseBlockStoreTestModule[YId] 
    with StubStorageModule[YId] {

  implicit val M: Monad[YId] with Copointed[YId] = YId.M //Trampoline.trampolineMonad

  val projections = Map.empty[ProjectionDescriptor, Projection]
  
  private var initialIndices = collection.mutable.Map[Path, Int]()    // if we were doing this for real: j.u.c.HashMap
  private var currentIndex = 0                                        // if we were doing this for real: j.u.c.a.AtomicInteger
  private val indexLock = new AnyRef                                  // if we were doing this for real: DIE IN A FIRE!!!
  
  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  override def load(table: Table, uid: UserId, jtpe: JType) = {
    table.toJson map { events =>
      fromJson {
        events.toStream flatMap {
          case JString(pathStr) => indexLock synchronized {      // block the WHOLE WORLD
            val path = Path(pathStr)
      
            val index = initialIndices get path getOrElse {
              initialIndices += (path -> currentIndex)
              currentIndex
            }
            
            val target = path.path.replaceAll("/$", ".json")
            val src = io.Source fromInputStream getClass.getResourceAsStream(target)
            val JArray(parsed) = JsonParser.parse(src.getLines.mkString(" "))
            
            currentIndex += parsed.length
            
            parsed.toStream zip (Stream from index) map {
              case (value, id) => JObject(JField("key", JArray(JNum(id) :: Nil)) :: JField("value", value) :: Nil)
            }
          }

          case x => sys.error("Attempted to load JSON as a table from something that wasn't a string: " + x)
        }
      }
    } 
  }
}

class FuturePlatformSpecs extends PlatformSpecs[Future] 
    with SystemActorStorageModule 
    with StandaloneShardSystemActorModule 
    with JDBMProjectionModule {

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  class Storage extends SystemActorStorageLike(fileMetadataStorage) {
    val accessControl = new UnlimitedAccessControl[Future]
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      fileMetadataStorage.findDescriptorRoot(descriptor, false)
    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      fileMetadataStorage.findArchiveRoot(descriptor)
  }

  type TableCompanion = BlockStoreColumnarTableCompanion
  object Table extends BlockStoreColumnarTableCompanion {
    //override def apply(slices: StreamT[M, Slice]) = super.apply(slices map { s => if (s.size != 96) s else sys.error("Slice size seen as 96 for the first time.") })
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  override def startup() {
    // start storage shard 
    Await.result(storage.start(), controlTimeout)
  }
  
  override def shutdown() {
    // stop storage shard
    Await.result(storage.stop(), controlTimeout)
    
    actorSystem.shutdown()
  }
}

