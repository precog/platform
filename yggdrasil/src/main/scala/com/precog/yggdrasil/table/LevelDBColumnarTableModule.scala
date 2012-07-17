package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.bytecode._
import Schema._

import akka.dispatch.{ExecutionContext,Future}

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scalaz.std.set._
import scalaz.syntax.monoid._


trait LevelDBTableConfig {
}

trait LevelDBColumnarTableModule extends ColumnarTableModule with StorageModule {
  type Projection <: BlockProjectionLike[Slice]
  type YggConfig <: LevelDBTableConfig

  protected implicit def executionContext: ExecutionContext

  class Table(slices: Iterable[Slice]) extends ColumnarTable(slices) {
    def load(tpe: JType): Future[Table] = {
      val paths: Set[String] = reduce {
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

      def loadable(path: Path, prefix: JPath, jtpe: JType): Future[Set[ProjectionDescriptor]] = {
        tpe match {
          case p: JPrimitiveType => Future.sequence(ctypes(p).map(metadataView.findProjections(path, prefix, _))) map {
            sources => sources flatMap { source => source.keySet }
          }

          case JArrayFixedT(elements) =>
            Future.sequence(elements map { case (i, jtpe) => loadable(path, prefix \ i, jtpe) }) map { _.flatten.toSet }

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
            Future.sequence(fields map { case (n, jtpe) => loadable(path, prefix \ n, jtpe) }) map { _.flatten.toSet }

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
            Future.sequence(Set(loadable(path, prefix, tpe1), loadable(path, prefix, tpe2))) map { _.flatten }
        }
      }

      def minimalCover(descriptors: Set[ProjectionDescriptor]): Set[ProjectionDescriptor] = sys.error("margin to small")

      def coveringSchema(descriptors: Set[ProjectionDescriptor]): Seq[(JPath, CType)] = sys.error("todo")

      for {
        coveringProjections <- Future.sequence(paths.map { path => loadable(Path(path), JPath.Identity, tpe) }) map { _.flatten }
                               if (subsumes(coveringSchema(coveringProjections), tpe))
      } yield {
        val loadableProjections = minimalCover(coveringProjections)
        new Table(
          new Iterable[Slice] {
            def iterator = new Iterator[Slice] {
              def hasNext: Boolean = sys.error("todo")
              def next: Slice = sys.error("todo")
            }
          }
        )
      }
    }
  }

  def table(slices: Iterable[Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
