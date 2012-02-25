package com.precog.yggdrasil
package kafka

import com.precog.util._
import leveldb._
import shard._
import Bijection._

import com.precog.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import _root_.kafka.consumer._

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection._

import scalaz._
import scalaz.syntax.std.booleanV._
import scalaz.syntax.std.optionV._
import scalaz.syntax.validation._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT
import scalaz.MonadPartialOrder._

case object Stop

case class ProjectionInsert(identities: Identities, values: Seq[CValue])

case object ProjectionGet

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator : EnumeratorT[Unit, Seq[CValue], IO]
}

class ProjectionActor(val projection: LevelDBProjection, descriptor: ProjectionDescriptor) extends Actor with Logging {
  def asCValue(jval: JValue): CValue = jval match { 
    case JString(s) => CString(s)
    case JInt(i)    => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b)   => CBoolean(b)
    case JNull      => CNull
    case x          => sys.error("JValue type not yet supported: " + x.getClass.getName )
  }

  def receive = {
    case Stop => //close the db
      projection.close.unsafePerformIO

    case ProjectionInsert(identities, values) => 
      //logger.debug("Projection insert")
      projection.insert(identities, values).unsafePerformIO
      sender ! ()

    case ProjectionGet => 
      sender ! projection
  }
}

// vim: set ts=4 sw=4 et:
