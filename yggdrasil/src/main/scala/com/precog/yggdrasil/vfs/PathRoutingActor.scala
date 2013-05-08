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
package com.precog
package yggdrasil
package vfs

import akka.actor.{Actor, ActorRef, Props}
import akka.dispatch.{Await, Future}
import akka.pattern.pipe
import akka.util.Duration


import blueeyes.bkka.FutureMonad
import blueeyes.util.Clock

import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.actor._
import com.precog.util._

import com.weiglewilczek.slf4s.Logging

import java.io.{IOException, File}

import scalaz._
import scalaz.std.stream._
import scalaz.effect.IO
import scalaz.syntax.id._
import scalaz.syntax.traverse._

class PathRoutingActor (baseDir: File, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration, jobManager: JobManager[Future], clock: Clock) extends Actor with Logging {
  private implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)

  private[this] var pathActors = Map.empty[Path, ActorRef]

  private[this] def targetActor(path: Path): IO[ActorRef] = {
    pathActors.get(path).map(IO(_)) getOrElse {
      val pathDir = VFSPathUtils.pathDir(baseDir, path)

      VersionLog.open(pathDir) map {
        _ map { versionLog =>
          context.actorOf(Props(new PathManagerActor(path, VFSPathUtils.versionsSubdir(pathDir), versionLog, resources, permissionsFinder, shutdownTimeout, jobManager, clock))) unsafeTap { newActor =>
            pathActors += (path -> newActor)
          }
        } valueOr { error =>
          throw new Exception(error.message)
        }
      }
    }
  }

  def receive = {
    case FindChildren(path, apiKey) =>
      VFSPathUtils.findChildren(baseDir, path, apiKey, permissionsFinder) pipeTo sender

    case IngestData(messages) =>
      val requestor = sender
      val groupedAndPermissioned = messages.groupBy({ case (_, event) => event.path }).toStream traverse {
        case (path, pathMessages) =>
          pathMessages.map(_._2.apiKey).distinct.toStream traverse { apiKey =>
            permissionsFinder.writePermissions(apiKey, path, clock.instant()) map { perms =>
              apiKey -> perms.toSet[Permission]
            }
          } map { allPerms =>
            targetActor(path) map { pathActor =>
              pathActor.tell(IngestBundle(pathMessages, allPerms.toMap), requestor)
            } except {
              case t: Throwable => IO(logger.error("Failure during version log open on " + path, t))
            }
          }
      }

      groupedAndPermissioned.foreach(_.sequence.unsafePerformIO)
  }
}
