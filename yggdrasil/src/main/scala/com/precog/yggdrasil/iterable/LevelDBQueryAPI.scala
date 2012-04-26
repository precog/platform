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

trait LevelDBQueryComponent extends StorageEngineQueryComponent with DatasetOpsComponent with YggConfigComponent with YggShardComponent[IterableDataset[Seq[CValue]]] {
  type Dataset[α] = IterableDataset[α]
  type YggConfig <: LevelDBQueryConfig

  implicit def asyncContext: akka.dispatch.ExecutionContext
  
  class QueryAPI extends LevelDBProjectionOps[IterableDataset[SValue]](yggConfig.clock, storage) with StorageEngineQueryAPI[IterableDataset] {
    def fullProjection(userUID: String, path: Path, expiresAt: Long): Dataset[SValue] = load(userUID, path, expiresAt)

    // pull each projection from the database, then for all the selectors that are provided
    // by tat projection, merge the values
    protected def retrieveAndJoin(path: Path, prefix: JPath, retrievals: Map[ProjectionDescriptor, Set[JPath]], expiresAt: Long): Future[IterableDataset[SValue]] = {
      def appendToObject(sv: SValue, instructions: Set[(JPath, Int)], cvalues: Seq[CValue]) = {
        instructions.foldLeft(sv) {
          case (sv, (selector, columnIndex)) => sv.set(selector, cvalues(columnIndex)).getOrElse(sv)
        }
      }

      def buildInstructions(descriptor: ProjectionDescriptor, selectors: Set[JPath]): (SValue, Set[(JPath, Int)]) = {
        Tuple2(
          selectors.flatMap(_.dropPrefix(prefix).flatMap(_.head)).toList match {
            case List(JPathField(_)) => SObject.Empty
            case List(JPathIndex(_)) => SArray.Empty
            case Nil => SNull
            case _ => sys.error("Inconsistent JSON structure: " + selectors)
          },
          selectors map { s =>
            (s.dropPrefix(prefix).get, descriptor.columns.indexWhere(col => col.path == path && s == col.selector)) 
          }
        )
      }

      def joinNext(retrievals: List[(ProjectionDescriptor, Set[JPath])]): Future[IterableDataset[SValue]] = retrievals match {
        case (descriptor, selectors) :: x :: xs => 
          val (init, instr) = buildInstructions(descriptor, selectors)
          for {
            projection <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
            dataset    <- joinNext(x :: xs)
          } yield {
            ops.extend(projection.getAllPairs(expiresAt)).cogroup(dataset) {
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
            projection <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
          } yield {
            val result = ops.extend(projection.getAllPairs(expiresAt)) map { appendToObject(init, instr, _) }
            result
          }
      }

      
      if (retrievals.isEmpty) Future(ops.empty[SValue](1)) else joinNext(retrievals.toList)
    }
  }
}

// vim: set ts=4 sw=4 et:
