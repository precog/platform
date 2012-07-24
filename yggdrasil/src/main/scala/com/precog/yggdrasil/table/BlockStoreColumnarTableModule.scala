package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.bytecode._
import Schema._

import akka.dispatch.{ExecutionContext,Future}

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scalaz._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.stream._

trait BlockStoreColumnarTableModule[M[+_]] extends ColumnarTableModule[M] with StorageModule[M] {
  type Projection <: BlockProjectionLike[Slice]

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    def load(tpe: JType): M[Table] = {
      val pathsM: M[Set[String]] = reduce {
        new CReducer[Set[String]] {
          def reduce(columns: JType => Set[Column], range: Range): Set[String] = {
            columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(s)
              case _ => Set()
            }
          }
        }
      }

      val metadataView = storage.userMetadataView(sys.error("TODO"))

      def loadable(path: Path, prefix: JPath, jtpe: JType): M[Set[ProjectionDescriptor]] = {
        tpe match {
          case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map {
            sources => sources flatMap { source => source.keySet }
          }

          case JArrayFixedT(elements) =>
            (elements map { case (i, jtpe) => loadable(path, prefix \ i, jtpe) }).toStream.sequence map { _.flatten.toSet }

          case JArrayUnfixedT =>
            metadataView.findProjections(path, prefix) map { 
              _.keySet filter { 
                _.columns exists { 
                  case ColumnDescriptor(`path`, selector, _, _) => 
                    (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[JPathIndex])
                }
              }
            }

          case JObjectFixedT(fields) =>
            (fields map { case (n, jtpe) => loadable(path, prefix \ n, jtpe) }).toStream.sequence map { _.flatten.toSet }

          case JObjectUnfixedT =>
            metadataView.findProjections(path, prefix) map { 
              _.keySet filter { 
                _.columns exists { 
                  case ColumnDescriptor(`path`, selector, _, _) => 
                    (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[JPathField])
                }
              }
            }

          case JUnionT(tpe1, tpe2) =>
            (Set(loadable(path, prefix, tpe1), loadable(path, prefix, tpe2))).sequence map { _.flatten }
        }
      }

      def minimalCover(descriptors: Set[ProjectionDescriptor]): Set[ProjectionDescriptor] = sys.error("margin to small")

      def coveringSchema(descriptors: Set[ProjectionDescriptor]): Seq[(JPath, CType)] = sys.error("todo")

      for {
        paths               <- pathsM
        coveringProjections <- (paths map { path => loadable(Path(path), JPath.Identity, tpe) }).sequence map { _.flatten }
      } yield {
        val loadableProjections = minimalCover(coveringProjections)
        table(
          sys.error("todo")
        )
      }
    }
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
