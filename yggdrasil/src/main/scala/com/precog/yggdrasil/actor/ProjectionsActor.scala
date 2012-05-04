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
package actor

import leveldb._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import java.io.File
import java.util.concurrent.TimeUnit

import scalaz._
import scalaz.syntax.std.optionV._
import scalaz.effect._

import scala.collection.mutable.ListBuffer

case class AcquireProjection(descriptor: ProjectionDescriptor)
case class AcquireProjectionBatch(descriptors: Iterable[ProjectionDescriptor])
case class ReleaseProjection(descriptor: ProjectionDescriptor) 
case class ReleaseProjectionBatch(descriptors: Seq[ProjectionDescriptor]) 

trait ProjectionResult

case class ProjectionAcquired(proj: ActorRef) extends ProjectionResult
case class ProjectionBatchAcquired(projs: Map[ProjectionDescriptor, ActorRef]) extends ProjectionResult
case class ProjectionError(ex: NonEmptyList[Throwable]) extends ProjectionResult

class ProjectionActors(descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO, scheduler: Scheduler) extends Actor with Logging {

  def receive = {

    case Status =>
      val status = JObject.empty ++
        JField("Projections", JObject.empty ++ JField("cacheSize", JInt(projectionActors.size)))
      sender ! status 

    case AcquireProjection(descriptor: ProjectionDescriptor) =>
      val proj = projectionActor(descriptor)
      mark(proj)
      sender ! proj
    
    case AcquireProjectionBatch(descriptors) =>
      var result = Map.empty[ProjectionDescriptor, ActorRef] 
      var errors = ListBuffer.empty[ProjectionError]
      
      val descItr = descriptors.iterator
     
      while(descItr.hasNext && errors.size == 0) {
        val desc = descItr.next
        val proj = projectionActor(desc)
        mark(proj)
        proj match {
          case ProjectionAcquired(proj) => result += (desc -> proj)
          case pe @ ProjectionError(_)  => errors += pe
        }
      }

      if(errors.size == 0) {
        sender ! ProjectionBatchAcquired(result) 
      } else {
        sender ! errors(0)
      }

    case ReleaseProjection(descriptor: ProjectionDescriptor) =>
      unmark(projectionActor(descriptor))
      sender ! ()
    
    case ReleaseProjectionBatch(descriptors: Seq[ProjectionDescriptor]) =>
      var cnt = 0
      while(cnt < descriptors.length) {
        unmark(projectionActor(descriptors(cnt)))
        cnt += 1
      }
      sender ! ()
  }

  def mark(result: ProjectionResult): Unit = result match {
    case ProjectionAcquired(proj) => proj ! IncrementRefCount
    case _                        =>
  }

  def unmark(result: ProjectionResult): Unit = result match {
    case ProjectionAcquired(proj) => proj ! DecrementRefCount
    case _                        =>
  }
  
  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => descriptorIO(descriptor).map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  private def projectionActor(descriptor: ProjectionDescriptor): ProjectionResult = {
    import ProjectionActors._
    val actor = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
      LevelDBProjection(initDescriptor(descriptor).unsafePerformIO, descriptor).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor, scheduler))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    actor
  }

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }

}

object ProjectionActors {
  implicit def validationToResult(validation: ValidationNEL[Throwable, ActorRef]): ProjectionResult = validation match {
    case Success(proj) => ProjectionAcquired(proj)
    case Failure(exs) => ProjectionError(exs)
  }
}

