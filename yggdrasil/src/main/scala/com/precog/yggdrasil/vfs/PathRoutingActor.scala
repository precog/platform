package com.precog
package yggdrasil
package vfs

import akka.actor.{Actor, ActorRef, Props}
import akka.dispatch.{Await, Future}
import akka.pattern.pipe
import akka.util.Duration


import blueeyes.util.Clock

import com.precog.common.Path
import com.precog.common.security.PermissionsFinder
import com.precog.common.jobs._
import com.precog.util._

import com.weiglewilczek.slf4s.Logging

import java.io.{IOException, File}

import scalaz.effect.IO
import scalaz.syntax.id._

class PathRoutingActor (baseDir: File, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration, jobManager: JobManager[Future], clock: Clock) extends Actor with Logging {
  private[this] var pathActors = Map.empty[Path, ActorRef]

  private[this] def targetActor(path: Path): IO[ActorRef] = {
    pathActors.get(path).map(IO(_)) getOrElse {
      val pathDir = VFSPathUtils.pathDir(baseDir, path)

      VersionLog.open(pathDir) map {
        _ map { versionLog =>
          context.actorOf(Props(new PathManagerActor(path, VFSPathUtils.versionsDir(pathDir), versionLog, resources, permissionsFinder, shutdownTimeout, jobManager, clock))) unsafeTap { newActor =>
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

    case pathOp : PathOp =>
      val requestor = sender

      val send = targetActor(pathOp.path) map {
        _.tell(pathOp, requestor)
      } except {
        case t: Throwable => IO(logger.error("Failure during version log open on " + pathOp.path, t))
      }

      send.unsafePerformIO
  }
}
