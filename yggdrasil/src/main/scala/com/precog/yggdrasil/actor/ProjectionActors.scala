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

import scala.collection.mutable.ListBuffer

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.syntax.std.optionV._


case class AcquireProjection(descriptor: ProjectionDescriptor)
case class AcquireProjectionBatch(descriptors: Iterable[ProjectionDescriptor])
case class ReleaseProjection(descriptor: ProjectionDescriptor) 
case class ReleaseProjectionBatch(descriptors: Seq[ProjectionDescriptor]) 

trait ProjectionResult

case class ProjectionAcquired(proj: ActorRef) extends ProjectionResult
case class ProjectionBatchAcquired(projs: Map[ProjectionDescriptor, ActorRef]) extends ProjectionResult
case class ProjectionError(ex: Throwable) extends ProjectionResult

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
      expirationPolicy = ExpirationPolicy(Some(2), Some(2), TimeUnit.MINUTES), 
      evict = { 
        (descriptor, actor) => descriptorIO(descriptor).map(_ => actor ! Stop(30)).unsafePerformIO
      }
    )
  )

  private def projectionActor(descriptor: ProjectionDescriptor): ProjectionResult = {
    val actor = projectionActors.get(descriptor) map { success[Throwable, ActorRef] } getOrElse {
      for (projection <- LevelDBProjection.forDescriptor(initDescriptor(descriptor).unsafePerformIO, descriptor)) yield {
        context.actorOf(Props(new ProjectionActor(projection, descriptor, scheduler)))
      } 
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    ProjectionActors.validationToResult(actor)
  }

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }
}

object ProjectionActors {
  def validationToResult(validation: Validation[Throwable, ActorRef]): ProjectionResult = validation match {
    case Success(proj) => ProjectionAcquired(proj)
    case Failure(exs) => ProjectionError(exs)
  }
}

