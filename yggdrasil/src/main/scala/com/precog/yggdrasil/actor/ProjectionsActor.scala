package com.precog.yggdrasil
package actor

import metadata._
import com.precog.util._
import com.precog.common._
import com.precog.common.ingest._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.table._
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

case class InsertComplete(path: Path)
case class ArchiveComplete(path: Path)
