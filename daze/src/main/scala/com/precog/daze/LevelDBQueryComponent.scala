package com.precog
package daze

import scala.annotation.tailrec

import com.precog.common.Path
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.leveldb._

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.duration._
import blueeyes.json.JPath
import blueeyes.json.JPathField
import blueeyes.json.JPathIndex
import blueeyes.util.Clock
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.std.set._

import com.weiglewilczek.slf4s.Logging

trait LevelDBQueryConfig {
  def clock: Clock
  def projectionRetrievalTimeout: akka.util.Timeout
}

trait LevelDBQueryComponent extends YggConfigComponent with StorageEngineQueryComponent with YggShardComponent with DatasetOpsComponent with Logging {
  type YggConfig <: LevelDBQueryConfig
  type Dataset[E] <: IterableDataset[E]
  import ops._

  implicit def asyncContext: ExecutionContext

  trait QueryAPI extends StorageEngineQueryAPI[Dataset] {
    type Sources = Set[(JPath, SType, ProjectionDescriptor)]

    /**
     *
     */
    override def fullProjection(userUID: String, path: Path, expiresAt: Long, release: Release): Dataset[SValue] = {
      logger.debug("Full projection on %s for %s from %s".format(path, userUID, storage))
      val dataset = Await.result(
        fullProjectionFuture(userUID, path, expiresAt, release),
        (expiresAt - yggConfig.clock.now().getMillis) millis
      )
      //logger.debug("Retrieved " + dataset)
      dataset
    }

    private def fullProjectionFuture(userUID: String, path: Path, expiresAt: Long, release: Release): Future[Dataset[SValue]] = {
      logger.debug("Full projection future on %s for %s from %s".format(path, userUID, storage))
      for {
        pathRoot <- storage.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) 
        dataset  <- assemble(path, JPath.Identity, sources(JPath.Identity, pathRoot), expiresAt, release)
      } yield {
        //logger.debug("fullProjectionFuture = " + dataset)
        dataset
      }
    }

    /**
     *
     */
    override def mask(userUID: String, path: Path): DatasetMask[Dataset] = {
      logger.debug("Mask %s for %s".format(path, userUID))
      LevelDBDatasetMask(userUID, path, None, None) 
    }

    private case class LevelDBDatasetMask(userUID: String, path: Path, selector: Option[JPath], tpe: Option[SType]) extends DatasetMask[Dataset] {
      def derefObject(field: String): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ field })

      def derefArray(index: Int): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ index })

      def typed(tpe: SType): DatasetMask[Dataset] = copy(tpe = Some(tpe))

      def realize(expiresAt: Long, release: Release): Dataset[SValue] = {
        logger.debug("Realizing %s:%s:%s for %s".format(path, selector, tpe, userUID))
        Await.result(
        (selector, tpe) match {
          case (Some(s), None | Some(SObject) | Some(SArray)) => 
            storage.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot => 
              assemble(path, s, sources(s, pathRoot), expiresAt, release)
            }

          case (Some(s), Some(tpe)) => 
            storage.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot =>
              assemble(path, s, sources(s, pathRoot) filter { 
                case (_, `tpe`, _) => true
                case _ => false
              }, expiresAt, release)
            }

          case (None   , Some(tpe)) if tpe != SObject && tpe != SArray => 
            storage.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) flatMap { pathRoot =>
              assemble(path, JPath.Identity, sources(JPath.Identity, pathRoot) filter { 
                case (_, `tpe`, _) => true 
                case _ => false
              }, expiresAt, release)
            }

          case (_      , _        ) => fullProjectionFuture(userUID, path, expiresAt, release)
        },
        (expiresAt - yggConfig.clock.now().getMillis) millis
      )
    }
    }

    def sources(selector: JPath, root: PathRoot): Sources = {
      logger.debug("Find sources for " + selector + " in " + root)
      def search(metadata: PathMetadata, selector: JPath, acc: Set[(JPath, SType, ProjectionDescriptor)]): Sources = {
        metadata match {
          case PathField(name, children) =>
            children.flatMap(search(_, selector \ name, acc))

          case PathIndex(idx, children) =>
            children.flatMap(search(_, selector \ idx, acc))

          case PathValue(valueType, _, descriptors) => 
            descriptors.headOption map { case (d, _) => acc + ((selector, valueType.stype, d)) } getOrElse acc
        }
      }

      root.children.flatMap(search(_, selector, Set.empty[(JPath, SType, ProjectionDescriptor)]))
    }

    def assemble(path: Path, prefix: JPath, sources: Sources, expiresAt: Long, release: Release)(implicit asyncContext: ExecutionContext): Future[Dataset[SValue]] = {
      logger.debug("Assembling " + path + " from " + sources)
      // pull each projection from the database, then for all the selectors that are provided
      // by that projection, merge the values
      def retrieveAndJoin(retrievals: Map[ProjectionDescriptor, Set[JPath]]): Future[Dataset[SValue]] = {
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

        def joinNext(retrievals: List[(ProjectionDescriptor, Set[JPath])]): Future[Dataset[SValue]] = retrievals match {
          case (descriptor, selectors) :: x :: xs => 
            val (init, instr) = buildInstructions(descriptor, selectors)
            for {
              (projection, prelease) <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
              dataset    <- joinNext(x :: xs)
            } yield {
              release += prelease.release
              val result = projection.getAllPairs(expiresAt).cogroup(dataset) {
                new CogroupF[Seq[CValue], SValue, SValue] {
                  def left(l: Seq[CValue]) = appendToObject(init, instr, l)
                  def both(l: Seq[CValue], r: SValue) = appendToObject(r, instr, l)
                  def right(r: SValue) = r
                }
              }
              result
            }

          case (descriptor, selectors) :: Nil =>
            val (init, instr) = buildInstructions(descriptor, selectors)
            for {
              (projection, prelease) <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
            } yield {
              release += prelease.release
              ops.extend(projection.getAllPairs(expiresAt)) map { appendToObject(init, instr, _) }
            }
        }

        if (retrievals.isEmpty) Future(ops.empty[SValue](1)) else joinNext(retrievals.toList)
      }

      // determine the projections from which to retrieve data
      // todo: for right now, this is implemented greedily such that the first
      // projection containing a desired column wins. It should be implemented
      // to choose the projection that satisfies the largest number of columns.
      val minimalDescriptors = sources.foldLeft(Map.empty[JPath, Set[ProjectionDescriptor]]) {
        case (acc, (selector, _, descriptor)) => 
          acc.get(selector) match {
            case Some(chosen) if chosen.contains(descriptor) ||
                                 (chosen exists { d => descriptor.columnAt(path, selector).exists(d.satisfies) }) => acc

            case _ => acc + (selector -> (acc.getOrElse(selector, Set.empty[ProjectionDescriptor]) + descriptor))
          }
      }

      val retrievals = minimalDescriptors.foldLeft(Map.empty[ProjectionDescriptor, Set[JPath]]) {
        case (acc, (jpath, descriptors)) => descriptors.foldLeft(acc) {
          (acc, descriptor) => acc + (descriptor -> (acc.getOrElse(descriptor, Set.empty[JPath]) + jpath))
        }
      }

      retrieveAndJoin(retrievals)
    }
  }
}
// vim: set ts=4 sw=4 et:
