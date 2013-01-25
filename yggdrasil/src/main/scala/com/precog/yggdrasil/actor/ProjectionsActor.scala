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
package actor

import metadata._
import com.precog.util._
import com.precog.common._
import com.precog.common.ingest._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.util._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.{Duration, DurationLong, Timeout}
import akka.util.duration._

import blueeyes.json._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import org.slf4j._

import java.io.File
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.id._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

//////////////
// MESSAGES //
//////////////

// To simplify routing, tag all messages for the ProjectionLike actor
sealed trait ShardProjectionAction {
  def descriptor: ProjectionDescriptor
}

sealed trait ProjectionUpdate extends ShardProjectionAction {
  def fold[A](insertf: ProjectionInsert => A, archivef: ProjectionArchive => A): A
}

case class ProjectionInsert(descriptor: ProjectionDescriptor, rows: Seq[ProjectionInsert.Row]) extends ProjectionUpdate {
  def fold[A](insertf: ProjectionInsert => A, archivef: ProjectionArchive => A): A = insertf(this)

  def toJValues: Seq[JValue] = {
    rows map { row =>
      assert(descriptor.columns.size == row.values.size)
      (descriptor.columns zip row.values).foldLeft[JValue](JObject(Nil)) {
        case (acc, (colDesc, cv)) => {
          CPathUtils.cPathToJPaths(colDesc.selector, cv).foldLeft(acc) {
            case (acc, (path, value)) => acc.set(path, value.toJValue)
          }
        }
      }
    }
  }
}

object ProjectionInsert {
  case class Row(id: EventId, values: Seq[CValue], metadata: Seq[Set[Metadata]])
}

case class ProjectionArchive(descriptor: ProjectionDescriptor, id: EventId) extends ProjectionUpdate {
  def fold[A](insertf: ProjectionInsert => A, archivef: ProjectionArchive => A): A = archivef(this)
}

case class ProjectionRequest(descriptor: ProjectionDescriptor) extends ShardProjectionAction

/////////////////

case object ProjectionIdleTimeout 

/////////////////

case class InsertMetadata(descriptor: ProjectionDescriptor, metadata: ColumnMetadata)
case class ArchiveMetadata(descriptor: ProjectionDescriptor)
case object InsertNoMetadata

////////////////
trait ActorProjectionModuleConfig extends BaseConfig {
  def projectionRetrievalTimeout: Timeout = config[Long]("actors.store.idle_projection_seconds", 300) seconds
  def idleProjectionDelay: Duration = config[Long]("actors.store.idle_projection_seconds", 300) seconds
}

trait ActorProjectionModule[Key, Block] extends ProjectionModule[Future, Key, Block] with YggConfigComponent { self =>
  type YggConfig <: ActorProjectionModuleConfig 

  val rawProjectionModule: RawProjectionModule[IO, Key, Block]

  case class ProjectionGetBlock(descriptor: ProjectionDescriptor, id: Option[Key], columns: Set[ColumnDescriptor])

  final class Projection(val descriptor: ProjectionDescriptor, val actorRef: ActorRef, timeout: Timeout) extends ProjectionLike[Future, Key, Block] {
    private implicit val reqTimeout = timeout
    // TODO: need Monad[M] @@ AccountResource
    def getBlockAfter(id: Option[Key], columns: Set[ColumnDescriptor] = Set())(implicit M: Monad[Future]): Future[Option[BlockProjectionData[Key, Block]]] = {
      (actorRef ? ProjectionGetBlock(descriptor, id, columns)) flatMap { a => M.point(a.asInstanceOf[Option[BlockProjectionData[Key, Block]]]) }
    }
  }

  class ProjectionCompanion(projectionsActor: ActorRef, timeout: Timeout) extends ProjectionCompanionLike[Future] {
    implicit val reqTimeout = timeout
    def apply(descriptor: ProjectionDescriptor): Future[Projection] = {
      (projectionsActor ? ProjectionRequest(descriptor)).mapTo[Projection]
    }
  }

  ////////////
  // ACTORS //
  ////////////

  class ProjectionsActor extends Actor { self =>
    private lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionsActor")

    private val projectionActors = mutable.Map.empty[ProjectionDescriptor, Projection]

    private def getProjection(descriptor: ProjectionDescriptor): Projection = {
      projectionActors.getOrElseUpdate(
        descriptor, 
        new Projection(
          descriptor, 
          context.actorOf(Props(new ProjectionActor(context.system.scheduler, descriptor, yggConfig.idleProjectionDelay))), 
          yggConfig.projectionRetrievalTimeout
        )
      )
    }

    def receive = {
      case Status =>
        sender ! status

      case ProjectionRequest(desc) => 
        sender ! getProjection(desc)

      case msg: ShardProjectionAction => 
        getProjection(msg.descriptor).actorRef.tell(msg, sender)
    }

    override def postStop(): Unit = {
      // TODO: Should this wait for all projections to close?
      projectionActors.values foreach { _.actorRef ! PoisonPill }
    }

    protected def status =  JObject(JField("Projections", JObject(JField("cacheSize", JNum(projectionActors.size)))))
  }

    /**
     * A per-projection worker Actor
     */
  class ProjectionActor(scheduler: Scheduler, descriptor: ProjectionDescriptor, maxIdleTime: Duration) extends Actor { self =>
    private val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionActor")

    private var projection: Option[rawProjectionModule.Projection] = None

    private var lastAccess: Long = System.currentTimeMillis 

    private def now = System.currentTimeMillis
    private def scheduleTimeout = scheduler.scheduleOnce((new DurationLong(lastAccess + maxIdleTime.toMillis - now)).millisecond, self, ProjectionIdleTimeout)

    override def preStart() = {
      scheduleTimeout
    }

    override def postStop() = {
      sleep.unsafePerformIO
    }

    private def awaken: IO[rawProjectionModule.Projection] = {
      val lastAccess0 = lastAccess
      val now0 = now
      for {
        _ <- IO { lastAccess = now0 }
        p <- projection.map(IO(_)) getOrElse { 
               logger.info("Reopening projection for %s at %d after %d idle.".format(descriptor.shows, now0, now0 - lastAccess0))
               rawProjectionModule.Projection(descriptor) map { _ tap { p => projection = Some(p) } } 
             }
      } yield p
    }

    private def sleep: IO[PrecogUnit] = {
      logger.info("Closing projection for %s at %d.".format(descriptor.shows, now))
      (for {
        _ <- (projection map rawProjectionModule.Projection.close).sequence
        _ <- IO { projection = None }
      } yield PrecogUnit) except { t => 
        IO { logger.error("Error closing projection for %s.".format(descriptor), t); PrecogUnit } 
      }
    }

    private def runInsert(projection: rawProjectionModule.Projection, rows: Seq[ProjectionInsert.Row]): IO[PrecogUnit] = {
      if (rows.nonEmpty) {
        val row = rows.head
        projection.insert(Array(row.id.uid), row.values) flatMap { _ =>
          runInsert(projection, rows.tail)
        }
      } else {
        IO(PrecogUnit)
      }
    }

    def receive = {
      case ProjectionInsert(`descriptor`, rows) => 
        val startTime = System.currentTimeMillis
        (for {
          p <- awaken
          _ <- IO { logger.debug("Inserting " + rows.size + " rows into " + p) }
          _ <- runInsert(p, rows)
          _ <- IO { logger.trace("runInsert complete for " + p.descriptor.shows) }
          _ <- p.commit
        } yield {
          logger.debug("Insertion of %d rows in %d ms".format(rows.size, System.currentTimeMillis - startTime))
          sender ! InsertMetadata(descriptor, ProjectionMetadata.columnMetadata(descriptor, rows))
        }) except {
          case t: Throwable => IO { logger.error("Error during insert, aborting batch: " + descriptor.shows, t) }
        } unsafePerformIO

      case ProjectionArchive(`descriptor`, archive) => 
        logger.debug("Archiving " + descriptor)
        
        (for {
           _ <- sleep
           _ <- rawProjectionModule.Projection.archive(descriptor) 
        } yield {
          sender ! ArchiveMetadata(descriptor)
        }) except {
          case t: Throwable => IO { logger.error("Error during archive on %s".format(descriptor), t) }
        } unsafePerformIO

      case ProjectionGetBlock(`descriptor`, id, columns) => 
        logger.info("Getting block for %s from actor %s after index %s.".format(descriptor.shows, System.identityHashCode(self), id))
        (for {
          p <- awaken
          block <- p.getBlockAfter(id, columns)
        } yield {
          sender ! block 
        }) unsafePerformIO

      case ProjectionIdleTimeout =>
        if (lastAccess + maxIdleTime.toMillis < now) {
          sleep.unsafePerformIO
        } else {
          scheduleTimeout
        }
    }
  }
}

// vim: set ts=4 sw=4 et:

