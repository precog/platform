package com.precog
package yggdrasil
package vfs

import akka.actor.{Actor, ActorRef, Props}
import akka.dispatch.{Await, Future}
import akka.pattern.pipe
import akka.util.Duration

import com.precog.common.Path
import com.precog.common.security.PermissionsFinder
import com.precog.util._

import com.weiglewilczek.slf4s.Logging

import java.io.{IOException, File}

class PathRoutingActor (baseDir: File, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration) extends Actor with Logging {
  private[this] var pathActors = Map.empty[Path, ActorRef]

  private[this] def targetActor(path: Path): ActorRef = {
    pathActors.get(path) getOrElse {
      context.actorOf(Props(new PathManagerActor(path, VFSPathUtils.pathDir(baseDir, path), resources, permissionsFinder, shutdownTimeout))).tap { newActor =>
        pathActors += (path -> newActor)
      }
    }
  }

  def receive = {
    case FindChildren(path, apiKey) =>
      VFSPathUtils.findChildren(baseDir, path, apiKey, permissionsFinder) pipeTo sender

    case pathOp : PathOp =>
      targetActor(pathOp.path).tell(pathOp, sender)
  }
}
