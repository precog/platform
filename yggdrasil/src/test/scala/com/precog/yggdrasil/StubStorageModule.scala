package com.precog.yggdrasil

import actor._
import metadata._
import util._
import SValue._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.util._

import com.precog.util._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json._

import scalaz._
import scalaz.effect._
import scalaz.syntax.std.boolean._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

trait StubProjectionModule[M[+_], Key, Block] extends ProjectionModule[M, Key, Block] { self =>
  implicit def M: Monad[M]

  protected def projections: Map[Path, Projection]

  class ProjectionCompanion extends ProjectionCompanionLike[M] {
    def apply(path: Path) = M.point(projections.get(path))
  }
}
