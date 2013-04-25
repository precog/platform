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
