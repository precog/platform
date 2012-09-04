package com.precog.yggdrasil
package table

import com.precog.common.{Path,VectorCase}
import com.precog.bytecode._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util._
import Schema._
import metadata._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import java.io.File
import java.util.SortedMap
import java.util.Comparator

import org.apache.jdbm.DBMaker

import com.weiglewilczek.slf4s.Logger

import scalaz._
import scalaz.Ordering._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

trait BlockStoreColumnarTableModule[M[+_]] extends
  ColumnarTableModule[M] with
  StorageModule[M] with
  IdSourceScannerModule[M] { self =>
    
  override type UserId = String
  type Key
  type Projection <: BlockProjectionLike[Key, Slice]
  type BD = BlockProjectionData[Key,Slice]
  
  def newMemoContext = new MemoContext
  
  class MemoContext extends MemoizationContext{
    import trans._
    
    private val memoCache = mutable.HashMap.empty[MemoId, M[Table]]
    
    private val memoKey   = "MemoKey"
    private val memoValue = "MemoValue"
    
    def memoize(table: Table, memoId: MemoId): M[Table] = {
      val preMemoTable =
        table.transform(
          ObjectConcat(
            WrapObject(
              Scan(
                ConstLiteral(CLong(0), Leaf(Source)),
                freshIdScanner),
              memoKey),
            WrapObject(
              Leaf(Source),
              memoValue
            )
          )
        )
      
      val memoTable = sort(preMemoTable, DerefObjectStatic(Leaf(Source), JPathField(memoKey)), SortAscending, memoId)
      M.map(memoTable) { _.transform(DerefObjectStatic(Leaf(Source), JPathField(memoValue))) }
    }
    
    def sort(table: Table, sortKey: TransSpec1, sortOrder: DesiredSortOrder, memoId: MemoId): M[Table] = {
      // yup, we still block the whole world. Yay.
      memoCache.synchronized {
        memoCache.get(memoId) match {
          case Some(memoTable) => memoTable
          case None =>
            val memoTable = table.sort(sortKey, sortOrder)
            memoCache += (memoId -> memoTable)
            memoTable
        }
      }
    }
    
    def expire(memoId: MemoId): Unit =
      memoCache.synchronized {
        memoCache -= memoId
      }
    
    def purge() : Unit =
      memoCache.synchronized {
        memoCache.clear()
      }
  }

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import trans._

    /** 
     * Determine the set of all projections that could potentially provide columns
     * representing the requested dataset.
     */
    def loadable(metadataView: StorageMetadata[M], path: Path, prefix: JPath, jtpe: JType): M[Set[ProjectionDescriptor]] = {
      jtpe match {
        case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map {
          sources => 
            sources flatMap { source => source.keySet }
        }

        case JArrayFixedT(elements) =>
          if (elements.isEmpty) {
            metadataView.findProjections(path, prefix, CEmptyArray) map { _.keySet }
          } else {
            (elements map { case (i, jtpe) => loadable(metadataView, path, prefix \ i, jtpe) } toSet).sequence map { _.flatten }
          }

        case JArrayUnfixedT =>
          metadataView.findProjections(path, prefix) map { sources =>
            sources.keySet filter { 
              _.columns exists { 
                case ColumnDescriptor(`path`, selector, _, _) => 
                  (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[JPathIndex])
              }
            }
          }

        case JObjectFixedT(fields) =>
          if (fields.isEmpty) {
            metadataView.findProjections(path, prefix, CEmptyObject) map { _.keySet }
          } else {
            (fields map { case (n, jtpe) => loadable(metadataView, path, prefix \ n, jtpe) } toSet).sequence map { _.flatten }
          }

        case JObjectUnfixedT =>
          metadataView.findProjections(path, prefix) map { sources =>
            sources.keySet filter { 
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

    class MergeEngine[KeyType,BlockData <: BlockProjectionData[KeyType,Slice]] {
      /**
       * A wrapper for a slice, and the function required to get the subsequent
       * block of data.
       */
      case class Cell(index: Int, maxKey: KeyType, slice0: Slice)(succ0: KeyType => M[Option[BlockData]]) {
        val uuid = java.util.UUID.randomUUID()
        private val remap = new Array[Int](slice0.size)
        var position: Int = 0

        def succ: M[Option[Cell]] = {
          for (blockOpt <- succ0(maxKey)) yield {
            blockOpt map { block => 
              Cell(index, block.maxKey, block.data)(succ0) 
            }
          }
        }

        def advance(i: Int): Boolean = {
          if (position < slice0.size) {
            remap(position) = i
            position += 1
          }
          
          position < slice0.size
        }
  
        def slice = {
          slice0.sparsen(remap, if (position > 0) remap(position - 1) + 1 else 0)
        }
  
        def split: (Slice, Cell) = {
          val (finished, continuing) = slice0.split(position)
          if (position == 0) {
            // If we never read from the slice, just return an empty slice and ourself 
            (finished, Cell(index, maxKey, continuing)(succ0))
          } else {
            (finished.sparsen(remap, remap(position - 1) + 1), Cell(index, maxKey, continuing)(succ0)) 
          }
        }
      }
  
      sealed trait CellMatrix { self => 
        def cells: Iterable[Cell]
        def compare(cl: Cell, cr: Cell): Ordering
        def refresh(index: Int, succ: M[Option[Cell]]): M[CellMatrix]
  
        implicit lazy val ordering = new scala.math.Ordering[Cell] {
          def compare(c1: Cell, c2: Cell) = self.compare(c1, c2).toInt
        }
      }
  
      object CellMatrix {
        def apply(initialCells: Set[Cell])(keyf: Slice => List[ColumnRef]): CellMatrix = {
          val size = initialCells.size
  
          type ComparatorMatrix = Array[Array[RowComparator]]
          def fillMatrix(initialCells: Set[Cell]): ComparatorMatrix = {
            val comparatorMatrix = Array.ofDim[RowComparator](initialCells.size, initialCells.size)
  
            for (Cell(i, _, s) <- initialCells; Cell(i0, _, s0) <- initialCells if i != i0) { 
              comparatorMatrix(i)(i0) = Slice.rowComparatorFor(s, s0)(keyf) 
            }
  
            comparatorMatrix
          }
  
          new CellMatrix { self =>
            private[this] val allCells: mutable.Map[Int, Cell] = initialCells.map(c => (c.index, c))(collection.breakOut)
            private[this] val comparatorMatrix = fillMatrix(initialCells)
  
            def cells = allCells.values
  
            def compare(cl: Cell, cr: Cell): Ordering = {
              comparatorMatrix(cl.index)(cr.index).compare(cl.position, cr.position)
            }
  
            def refresh(index: Int, succ: M[Option[Cell]]): M[CellMatrix] = {
              succ map {
                case Some(c @ Cell(i, _, s)) => 
                  allCells += (i -> c)
  
                  for ((_, Cell(i0, _, s0)) <- allCells if i0 != i) {
                    comparatorMatrix(i)(i0) = Slice.rowComparatorFor(s, s0)(keyf) 
                    comparatorMatrix(i0)(i) = Slice.rowComparatorFor(s0, s)(keyf)
                  }
  
                  self
                  
                case None => 
                  allCells -= index
  
                  // this is basically so that we'll fail fast if something screwy happens
                  for ((_, Cell(i0, _, s0)) <- allCells if i0 != index) {
                    comparatorMatrix(index)(i0) = null
                    comparatorMatrix(i0)(index) = null
                  }
  
                  self
              }
            }
          }
        }
      }

      def mergeProjections(cellsM: Set[M[Option[Cell]]], invert: Boolean = false)(keyf: Slice => List[ColumnRef]): M[Table] = {
        for (cells <- cellsM.sequence.map(_.flatten)) yield {
          val cellMatrix = CellMatrix(cells)(keyf)
  
          table(
            StreamT.unfoldM[M, Slice, mutable.PriorityQueue[Cell]](mutable.PriorityQueue(cells.toSeq: _*)(if (invert) cellMatrix.ordering.reverse else cellMatrix.ordering)) { queue =>
  
              // dequeues all equal elements from the head of the queue
              @inline @tailrec def dequeueEqual(cells: List[Cell]): List[Cell] = {
                if (queue.isEmpty) cells
                else if (cells.isEmpty || cellMatrix.compare(queue.head, cells.head) == EQ) dequeueEqual(queue.dequeue() :: cells)
                else cells
              }
  
              // consume as many records as possible
              @inline @tailrec def consumeToBoundary(idx: Int): (Int, List[Cell]) = {
                val cellBlock = dequeueEqual(Nil)
                
                if (cellBlock.isEmpty) {
                  // At the end of data, since this will only occur if nothing remains in the priority queue
                  (idx, Nil)
                } else {
                  val (continuing, expired) = cellBlock partition { _.advance(idx) }
                  queue.enqueue(continuing: _*)
  
                  if (expired.isEmpty) consumeToBoundary(idx + 1) else (idx + 1, expired)
                }
              }
  
              val (finishedSize, expired) = consumeToBoundary(0)
              if (expired.isEmpty) {
                M.point(None)
              } else {
                val completeSlices = expired.map(_.slice)

                val (prefixes, suffixes) = queue.dequeueAll.map(_.split).unzip

                val emission = new Slice {
                  val size = finishedSize
                  val columns: Map[ColumnRef, Column] = {
                    (completeSlices.flatMap(_.columns) ++ prefixes.flatMap(_.columns)).groupBy(_._1).map {
                      case (ref, columns) => {
                        val cp: Pair[ColumnRef, Column] = if (columns.size == 1) {
                          columns.head
                        } else {
                          (ref, ArraySetColumn(ref.ctype, columns.map(_._2).toArray))
                        }
                        cp
                      }
                    }
                  } 
                }

                val updatedMatrix = expired.foldLeft(M.point(cellMatrix)) {
                  case (matrixM, cell) => matrixM.flatMap(_.refresh(cell.index, cell.succ))
                }
  
                val updatedMatrix0 = suffixes.foldLeft(updatedMatrix) {
                  case (matrixM, cell) => matrixM.flatMap(_.refresh(cell.index, M.point(Some(cell))))
                }
  
                updatedMatrix0 map { matrix => 
                  val queue0 = mutable.PriorityQueue(matrix.cells.toSeq: _*)(matrix.ordering)
                  Some((emission, queue0))
                }
              }
            }
          )
        }
      }
    }

    private object loadMergeEngine extends MergeEngine[Key,BD]

    def load(uid: UserId, tpe: JType): M[Table] = {
      import loadMergeEngine._
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

      def cellsM(projections: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Set[M[Option[Cell]]] = for (((desc, cols), i) <- projections.toSeq.zipWithIndex.toSet) yield {
        val succ: Option[Key] => M[Option[BD]] = (key: Option[Key]) => storage.projection(desc) map {
          case (projection, release) => 
            val result = projection.getBlockAfter(key, cols)  
            release.release.unsafePerformIO
            result
        }

        succ(None) map { 
          _ map { nextBlock => Cell(i, nextBlock.maxKey, nextBlock.data) { k => succ(Some(k)) } }
        }
      }

      for {
        paths               <- pathsM
        coveringProjections <- (paths map { path => loadable(metadataView, path, JPath.Identity, tpe) }).sequence map { _.flatten }
        result              <- mergeProjections(cellsM(minimalCover(tpe, coveringProjections))) { slice => 
            //todo: How do we actually determine the correct function for retrieving the key?
            slice.columns.keys.filter( { case ColumnRef(selector, ctype) => selector.nodes.startsWith(JPathField("key") :: Nil) }).toList.sorted
          }
      } yield result
    }

    private type SortingKey = Array[Byte]
    private type SortBlockData = BlockProjectionData[SortingKey,Slice]
    private object sortMergeEngine extends MergeEngine[SortingKey, SortBlockData]

    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): M[Table] = {
      import sortMergeEngine._
      import TableModule.paths._

      // Bookkeeping types/case classes
      type IndexStore = SortedMap[SortingKey,Array[Byte]]
      case class SliceIndex(name: String, storage: IndexStore, keyComparator: Comparator[SortingKey], sortRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef])

      // Map a set of slice columns to a given JDBM DB index, since each slice could vary in column formats, names, etc
      type IndexMap = Map[Seq[ColumnRef], SliceIndex]

      case class SortOutput(indices: IndexMap, idCount: Int, globalId: Long)

      // We use the sort transspec1 to compute a new table with a combination of the 
      // original data and the new sort columns, referenced under the sortkey namespace
      val tableWithSortKey = transform(ObjectConcat(Leaf(Source), WrapObject(sortKey, SortKey.name)))

      // Open a JDBM3 DB for use in sorting under a temp directory
      val dbFile = new File(newScratchDir(), "sortspace")
      val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()

      // Insert slice data based on columndescriptors
      val inputOp: M[SortOutput] = tableWithSortKey.slices.foldLeft(SortOutput(Map.empty, 0, 0l)) { case (SortOutput(indices, _, nextId), slice) => {
        def columnsByPrefix(prefix: JPath, dropPrefix: Boolean): List[(ColumnRef,Column)] = slice.columns.collect {
          // The conditional guarantees that dropPrefix will return Some
          case pair @ (ColumnRef(selector, tpe), col) if selector.hasPrefix(prefix) =>
            if (dropPrefix) {
              (ColumnRef(selector.dropPrefix(prefix).get, tpe), col)
            } else pair
        }.toList

        val dataColumns = columnsByPrefix(JPath(Value), true)
        val idColumns   = columnsByPrefix(JPath(Key), false)
        val sortColumns = columnsByPrefix(JPath(SortKey), false)
        val ids = new Array[Long](slice.size)
        val nextAvailGlobalId = (0 until slice.size).foldLeft(nextId) { (nextId, row) =>
          if (dataColumns.exists(_._2.isDefinedAt(row))) {
            ids(row) = nextId
            nextId + 1
          } else {
            ids(row) = -1L
            nextId
          }
        }
        val globalIdColumn = ColumnRef(JPath(SortGlobalId), CLong) -> new LongColumn {
          def apply(row: Int) = ids(row)
          def isDefinedAt(row: Int) = ids(row) >= 0
        }
        val keyColumns = sortColumns ++ (idColumns :+ globalIdColumn)

        val indexMapKey = (sortColumns ++ dataColumns).map(_._1).toSeq

        val dataRowFormat = RowFormat.forValues(dataColumns map (_._1))
        val dataColumnEncoder = dataRowFormat.ColumnEncoder(dataColumns map (_._2))

        val keyRowFormat = RowFormat.forValues(keyColumns map (_._1))
        val keyColumnEncoder = keyRowFormat.ColumnEncoder(keyColumns map (_._2))
        val keyComparator = SortingKeyComparator(keyRowFormat, sortOrder.isAscending)

        val (index, newIndices) = indices.get(indexMapKey).map((_,indices)).getOrElse {
          val newIndex = SliceIndex(indexMapKey.toString,
                                    DB.createTreeMap(indexMapKey.toString, keyComparator, null, null),
                                    keyComparator,
                                    keyColumns.map(_._1),
                                    dataColumns.map(_._1))

          (newIndex, indices + (indexMapKey -> newIndex))
        }
        
        // Iterate over the slice
        // TODO why doesn't this work with a tailrec?
        var row = 0

        while (row < slice.size) {
          if (globalIdColumn._2.isDefinedAt(row)) {
            try {
              index.storage.put(keyColumnEncoder.encodeFromRow(row), dataColumnEncoder.encodeFromRow(row))
            } catch {
              case t: Throwable => println("Error on storeRow: " + t); throw t
            }

            val globalId = globalIdColumn._2(row)
            if (globalId % jdbmCommitInterval == 0 && globalId > 0) {
              DB.commit()
            }
          }
          row += 1
        }

        SortOutput(newIndices, idColumns.size, nextAvailGlobalId)
      }}.map {
        case output @ SortOutput(indices, idCount, lastId) => {
          DB.close()
          output
        }
      }

      // Merge the resulting slice indices back together
      inputOp.flatMap { 
        case SortOutput(_, _, 0l) => {
          // We've been asked to sort an empty table. Return the input
          M.point(this)
        }
        case SortOutput(indices, idCount, _) => {
          // Map the distinct indices into SortProjections/Cells, then merge them
          val cells: Set[M[Option[Cell]]] = indices.zipWithIndex.map {
            case ((_, SliceIndex(name, _, keyComparator, sortColumns, valColumns)), index) => {

              val sortProjection = new JDBMRawSortProjection(dbFile, name, idCount, sortColumns, valColumns) {
                // A little ugly, but getting this implicitly seemed a little crazy
                val keyOrder: Order[SortingKey] =
                  scalaz.Order.fromScalaOrdering(scala.math.Ordering.comparatorToOrdering(keyComparator))
              }

              val succ: Option[SortingKey] => M[Option[SortBlockData]] = (key: Option[SortingKey]) => M.point(sortProjection.getBlockAfter(key))

              succ(None) map { 
                _ map { nextBlock => Cell(index, nextBlock.maxKey, nextBlock.data) { k => succ(Some(k)) } }
              }
            }
          }.toSet

          mergeProjections(cells, sortOrder.isAscending) { slice => {
            // ensure that sort keys compare before identities
            slice.columns.keys.filter({ case ColumnRef(selector, _) => selector.nodes.startsWith(SortKey :: Nil) }).toList.sorted :::
            slice.columns.keys.filter({ case ColumnRef(selector, _) => selector.nodes.startsWith(Key :: Nil) }).toList.sorted :::
            slice.columns.keys.filter({ case ColumnRef(selector, _) => selector.nodes.startsWith(SortGlobalId :: Nil) }).toList.sorted
          }}
        }
      }
    }
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
