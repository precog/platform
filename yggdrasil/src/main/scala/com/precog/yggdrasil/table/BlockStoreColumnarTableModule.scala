package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.bytecode._
import Schema._
import metadata._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scalaz._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

trait BlockStoreColumnarTableModule[M[+_]] extends ColumnarTableModule[M] with StorageModule[M] {
  type UserId = String
  type Projection <: BlockProjectionLike[Slice]

  type BD = Projection#BlockData

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    /** 
     * Determine the set of all projections that could potentially provide columns
     * representing the requested dataset.
     */
    def loadable(metadataView: StorageMetadata[M], path: Path, prefix: JPath, jtpe: JType): M[Set[ProjectionDescriptor]] = {
      jtpe match {
        case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map {
          sources => sources flatMap { source => source.keySet }
        }

        case JArrayFixedT(elements) =>
          (elements map { case (i, jtpe) => loadable(metadataView, path, prefix \ i, jtpe) } toSet).sequence map { _.flatten }

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
          (fields map { case (n, jtpe) => loadable(metadataView, path, prefix \ n, jtpe) } toSet).sequence map { _.flatten }

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
          (Set(loadable(metadataView, path, prefix, tpe1), loadable(metadataView, path, prefix, tpe2))).sequence map { _.flatten }
      }
    }

    /**
     * Find the minimal set of projections (and the relevant columns from each projection) that
     * will be loaded to provide a dataset of the specified type.
     */
    def minimalCover(tpe: JType, descriptors: Set[ProjectionDescriptor]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
      @inline @tailrec
      def cover0(uncovered: Set[ColumnDescriptor], unused: Map[ProjectionDescriptor, Set[ColumnDescriptor]], covers: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
        if (uncovered.isEmpty) {
          covers
        } else {
          val (b0, covered) = unused map { case (b, dcols) => (b, dcols & uncovered) } maxBy { _._2.size } 
          cover0(uncovered &~ covered, unused - b0, covers + (b0 -> covered))
        }
      }

      cover0(
        descriptors.flatMap(_.columns.toSet) filter { cd => includes(tpe, cd.selector, cd.valueType) }, 
        descriptors map { b => (b, b.columns.toSet) } toMap, 
        Map.empty)
    }

    /**
     * A wrapper for a slice, and the function required to get the subsequent
     * block of data.
     */
    case class Cell(index: Int, block: BD)(succ0: BD => M[Option[BD]]) {
      var position: Int = 0

      def succ: M[Option[Cell]] = {
        for (blockOpt <- succ0(block)) yield {
          blockOpt map { block => Cell(index, block)(succ0) }
        }
      }
    }

    sealed trait CellMatrix { self => 
      def compare(cl: Cell, cr: Cell): Ordering
      def refresh(cell: Cell): M[CellMatrix]

      implicit lazy val ordering = new scala.math.Ordering[Cell] {
        def compare(c1: Cell, c2: Cell) = self.compare(c1, c2).toInt
      }
    }

    object CellMatrix {
      def apply(cells: Set[Cell])(keyf: Slice => List[ColumnRef]): CellMatrix = {
        val size = cells.size

        type ComparatorMatrix = Array[Array[(Int, Int) => Ordering]]
        def fillMatrix(cells: Set[Cell]): ComparatorMatrix = {
          val comparatorMatrix = Array.ofDim[(Int, Int) => Ordering](cells.size, cells.size)

          for (Cell(i, b) <- cells; Cell(i0, b0) <- cells if i != i0) { 
            comparatorMatrix(i)(i0) = Slice.rowComparator(b.data, b0.data)(keyf) 
          }

          comparatorMatrix
        }

        new CellMatrix { self =>
          private[this] val allCells: mutable.Map[Int, Cell] = cells.map(c => (c.index, c))(collection.breakOut)
          private[this] val comparatorMatrix = fillMatrix(cells)

          def compare(cl: Cell, cr: Cell): Ordering = {
            comparatorMatrix(cl.index)(cr.index)(cl.position, cr.position)
          }

          def refresh(cell: Cell): M[CellMatrix] = {
            cell.succ map {
              case Some(c @ Cell(i, b)) => 
                allCells += (i -> c)

                for ((_, Cell(i0, b0)) <- allCells if i0 != i) {
                  comparatorMatrix(i)(i0) = Slice.rowComparator(b.data, b0.data)(keyf) 
                  comparatorMatrix(i0)(i) = Slice.rowComparator(b0.data, b.data)(keyf)
                }

                self
                
              case None => 
                allCells -= cell.index

                // this is basically so that we'll fail fast if something screwy happens
                for ((_, Cell(i0, b0)) <- allCells if i0 != cell.index) {
                  comparatorMatrix(cell.index)(i0) = null
                  comparatorMatrix(i0)(cell.index) = null
                }

                self
            }
          }
        }
      }
    }

    def load(uid: UserId, tpe: JType): M[Table] = {
      def load0(projections: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): M[Table] = {
        val cellsM: Set[M[Option[Cell]]] = for (((desc, cols), i) <- projections.toSeq.zipWithIndex.toSet) yield {
          val succ: Option[BD] => M[Option[BD]] = (block: Option[BD]) => storage.projection(desc) map {
            case (projection, release) => 
              val result = projection.getBlockAfter(block.map(_.maxKey.asInstanceOf[projection.Key]), cols)  //todo: remove asInstanceOf
              release.release.unsafePerformIO
              result
          }

          succ(None) map { 
            _ map { nextBlock => Cell(i, nextBlock) { b => succ(Some(b)) } }
          }
        }

        for (cells <- cellsM.sequence.map(_.flatten)) yield {
          val cellMatrix = CellMatrix(cells) {
            //todo: How do we actually determine the correct function for retrieving the key?
            slice => slice.columns.keys.filter { case ColumnRef(selector, ctype) => sys.error("todo") } toList
          }

          table(
            StreamT.unfoldM[M, Slice, mutable.PriorityQueue[Cell]](mutable.PriorityQueue(cells.toSeq: _*)(cellMatrix.ordering)) { queue =>
              sys.error("todo")
            }
          )
        }
      }

      val metadataView = storage.userMetadataView(uid)

      // Reduce this table to obtain the in-memory set of strings representing the vfs paths
      // to be loaded.
      val pathsM = this.reduce {
        new CReducer[Set[Path]] {
          def reduce(columns: JType => Set[Column], range: Range): Set[Path] = {
            columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _ => Set()
            }
          }
        }
      }

      for {
        paths               <- pathsM
        coveringProjections <- (paths map { path => loadable(metadataView, path, JPath.Identity, tpe) }).sequence map { _.flatten }
        result              <- load0(minimalCover(tpe, coveringProjections))
      } yield result
    }
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
