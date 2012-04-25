package com.precog
package yggdrasil
package actor

import leveldb._

import akka.actor.Actor
import akka.actor.Scheduler
import akka.util.duration._

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s._

import scala.annotation.tailrec
import scalaz.effect._
import scalaz.iteratee.EnumeratorT

case object Stop

case object IncrementRefCount
case object DecrementRefCount

case class ProjectionInsert(identities: Identities, values: Seq[CValue])
case class ProjectionBatchInsert(insert: Seq[ProjectionInsert])

case object ProjectionGet

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator : EnumeratorT[Unit, Seq[CValue], IO]
}

class ProjectionActor[Dataset](val projection: Projection[Dataset], scheduler: Scheduler) extends Actor with Logging {
  def asCValue(jval: JValue): CValue = jval match { 
    case JString(s) => CString(s)
    case JInt(i)    => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b)   => CBoolean(b)
    case x          => sys.error("JValue type not yet supported: " + x.getClass.getName )
  }

  var refCount = 0

  def insertAll(batch: Seq[ProjectionInsert]) = {
    @tailrec def step(iter: Iterator[ProjectionInsert]) {
      if (iter.hasNext) {
        val insert = iter.next
        projection.insert(insert.identities, insert.values)
        step(iter)
      }
    }

    step(batch.iterator)
  }

  def receive = {
    case Stop => //close the db
      if(refCount == 0) {
        logger.debug("Closing projection.")
        projection.close
      } else {
        logger.debug("Deferring close ref count [%d]".format(refCount))
        scheduler.scheduleOnce(1 second, self, Stop) 
      }

    case IncrementRefCount => refCount += 1

    case DecrementRefCount => refCount -= 1

    case ProjectionInsert(identities, values) => 
      //logger.debug("Projection insert")
      projection.insert(identities, values)
      sender ! ()
    
    case ProjectionBatchInsert(inserts) =>
      insertAll(inserts)
      sender ! ()

    case ProjectionGet => 
      sender ! projection
  }
}

// vim: set ts=4 sw=4 et:
