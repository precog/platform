package com.precog.yggdrasil
package iterable

import scala.annotation.tailrec

import leveldb._
import com.precog.common.Path

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.duration._
import blueeyes.json.JPath
import blueeyes.json.JPathField
import blueeyes.json.JPathIndex
import blueeyes.util.Clock

import scalaz._

trait LevelDBQueryConfig {
  def clock: Clock
  def projectionRetrievalTimeout: akka.util.Timeout
}

trait LevelDBQueryComponent extends StorageEngineQueryComponent with DatasetOpsComponent with YggConfigComponent with StorageModule[Future] {
  type Projection <: FullProjectionLike[Dataset[Seq[CValue]]]
  type YggConfig <: LevelDBQueryConfig

  implicit def asyncContext: akka.dispatch.ExecutionContext
  
  class QueryAPI extends LevelDBProjectionOps[Dataset[SValue]](yggConfig.clock, storage) with StorageEngineQueryAPI[Dataset] {
    def fullProjection(userUID: String, path: Path, expiresAt: Long, release: Release): Dataset[SValue] = load(userUID, path, expiresAt, release)

    // pull each projection from the database, then for all the selectors that are provided
    // by tat projection, merge the values
    protected def retrieveAndJoin(path: Path, prefix: JPath, retrievals: Map[ProjectionDescriptor, Set[JPath]], expiresAt: Long, release: Release): Future[Dataset[SValue]] = {
      def appendToObject(sv: SValue, instructions: Set[(CType, JPath, Int)], cvalues: Seq[CValue]) = {
        instructions.foldLeft(sv) {
          case (sv, (ctype, selector, columnIndex)) => 
            ctype match {
              case CEmptyObject => sv.set(selector, SObject.Empty).getOrElse(sv)
              case CEmptyArray => sv.set(selector, SArray.Empty).getOrElse(sv)
              case CNull => sv.set(selector, SNull).getOrElse(sv)
              case _ => sv.set(selector, cvalues(columnIndex)).getOrElse(sv)
            }
        }
      }

      def buildInstructions(descriptor: ProjectionDescriptor, selectors: Set[JPath]): (SValue, Set[(CType, JPath, Int)]) = {
        Tuple2(
          selectors.flatMap(_.dropPrefix(prefix).flatMap(_.head)).toList match {
            case List(JPathField(_)) => SObject.Empty
            case List(JPathIndex(_)) => SArray.Empty
            case Nil => SNull
            case _ => sys.error("Inconsistent JSON structure: " + selectors)
          },
          selectors map { s =>
            val columnIndex = descriptor.columns.indexWhere(col => col.path == path && s == col.selector)

            (descriptor.columns(columnIndex).valueType, s.dropPrefix(prefix).get, columnIndex)
          }
        )
      }

      def joinNext(retrievals: List[(ProjectionDescriptor, Set[JPath])]): Future[Dataset[SValue]] = retrievals match {
        case (descriptor, selectors) :: x :: xs => 
          val (init, instr) = buildInstructions(descriptor, selectors)
          for {
            (projection, prelease) <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
            dataset    <- joinNext(x :: xs)
          } yield {
            release += prelease.release
            ops.extend(projection.allRecords(expiresAt)).cogroup(dataset) {
              new CogroupF[Seq[CValue], SValue, SValue] {
                def left(l: Seq[CValue]) = appendToObject(init, instr, l)
                def both(l: Seq[CValue], r: SValue) = appendToObject(r, instr, l)
                def right(r: SValue) = r
              }
            }
          }

        case (descriptor, selectors) :: Nil =>
          val (init, instr) = buildInstructions(descriptor, selectors)
          for {
            (projection, prelease) <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
          } yield {
            release += prelease.release
            ops.extend(projection.allRecords(expiresAt)) map { appendToObject(init, instr, _) }
          }
      }

      
      if (retrievals.isEmpty) Future(ops.empty[SValue](1)) else joinNext(retrievals.toList)
    }
  }
}

// vim: set ts=4 sw=4 et:
