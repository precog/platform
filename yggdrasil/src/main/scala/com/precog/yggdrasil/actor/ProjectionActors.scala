package com.precog
package yggdrasil
package actor

import leveldb._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef

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

case object CheckMessages

case class AcquireProjection(descriptor: ProjectionDescriptor)
case class AcquireProjectionBatch(descriptors: Iterable[ProjectionDescriptor])
case class ReleaseProjection(descriptor: ProjectionDescriptor) 
case class ReleaseProjectionBatch(descriptors: Array[ProjectionDescriptor]) 

trait ProjectionResult
object ProjectionResult {
  def apply(validation: ValidationNEL[Throwable, ActorRef]): ProjectionResult = validation match {
    case Success(proj) => ProjectionAcquired(proj)
    case Failure(exs) => ProjectionError(exs)
  }
}

case class ProjectionAcquired(proj: ActorRef) extends ProjectionResult
case class ProjectionBatchAcquired(projs: Map[ProjectionDescriptor, ActorRef]) extends ProjectionResult
case class ProjectionError(ex: NonEmptyList[Throwable]) extends ProjectionResult

class ProjectionActors(projectionFactory: ProjectionFactory, descriptorStorage: ProjectionDescriptorStorage, scheduler: Scheduler) extends Actor with Logging {
  def receive = {
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
    
    case ReleaseProjectionBatch(descriptors: Array[ProjectionDescriptor]) =>
      var cnt = 0
      while(cnt < descriptors.length) {
        unmark(projectionActor(descriptors(cnt)))
        cnt += 1
      }
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
        (descriptor, actor) => descriptorStorage.saveDescriptor(descriptor).map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  private def projectionActor(descriptor: ProjectionDescriptor): ProjectionResult = {
    val actor = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
      projectionFactory.projection(descriptor).map(projection => context.actorOf(Props(new ProjectionActor(projection, scheduler))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    ProjectionResult(actor)
  }
}

