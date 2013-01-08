/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package table

import com.precog.common.{Path, VectorCase}
import com.precog.common.json._
import com.precog.bytecode._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util._

import com.precog.yggdrasil.table.cf.util.{Remap, Empty}

import TransSpecModule._

import blueeyes.bkka.AkkaTypeClasses
import blueeyes.json._

import org.apache.commons.collections.primitives.ArrayIntList
import org.joda.time.DateTime
import com.google.common.io.Files

import org.slf4j.Logger

import org.apache.jdbm.DBMaker
import java.io.File
import java.util.SortedMap

import com.precog.util.{BitSet, BitSetUtil, DateTimeUtil, IOUtils, Loop}
import com.precog.util.BitSetUtil.Implicits._

import scala.collection.mutable
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering._
import scalaz.std.function._
import scalaz.std.list._
import scalaz.std.tuple._
//import scalaz.std.iterable._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.std.vector._
import scalaz.syntax.arrow._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

import java.nio.CharBuffer

trait ColumnarTableTypes {
  type F1 = CF1
  type F2 = CF2
  type Scanner = CScanner
  type Reducer[α] = CReducer[α]
  type RowId = Int
}

trait ColumnarTableModuleConfig {
  def maxSliceSize: Int
  
  def maxSaneCrossSize: Long = 2400000000L    // 2.4 billion
}

trait ColumnarTableModule[M[+_]]
    extends TableModule[M]
    with ColumnarTableTypes
    with IdSourceScannerModule[M]
    with SliceTransforms[M]
    with SamplableColumnarTableModule[M]
    with IndicesModule[M]
    with YggConfigComponent {
      
  import TableModule._
  import trans._
  import trans.constants._
  
  type YggConfig <: IdSourceConfig with ColumnarTableModuleConfig

  type Table <: ColumnarTable
  type TableCompanion <: ColumnarTableCompanion
  case class TableMetrics(startCount: Int, sliceTraversedCount: Int)

  def newScratchDir(): File = IOUtils.createTmpDir("ctmscratch").unsafePerformIO
  def jdbmCommitInterval: Long = 200000l

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = CF1("builtin::liftF2::applyl") { f(Column.const(cv), _) } 
    def applyr(cv: CValue) = CF1("builtin::liftF2::applyl") { f(_, Column.const(cv)) }

    def andThen(f1: F1) = CF2("builtin::liftF2::andThen") { (c1, c2) => f(c1, c2) flatMap f1.apply }
  }

  trait ColumnarTableCompanion extends TableCompanionLike {
    def apply(slices: StreamT[M, Slice], size: TableSize): Table
    
    def singleton(slice: Slice): Table

    implicit def groupIdShow: Show[GroupId] = Show.showFromToString[GroupId]

    def empty: Table = Table(StreamT.empty[M, Slice], ExactSize(0))
    
    def constBoolean(v: collection.Set[CBoolean]): Table = {
      val column = ArrayBoolColumn(v.map(_.value).toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CBoolean) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constLong(v: collection.Set[CLong]): Table = {
      val column = ArrayLongColumn(v.map(_.value).toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CLong) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constDouble(v: collection.Set[CDouble]): Table = {
      val column = ArrayDoubleColumn(v.map(_.value).toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CDouble) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constDecimal(v: collection.Set[CNum]): Table = {
      val column = ArrayNumColumn(v.map(_.value).toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CNum) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constString(v: collection.Set[CString]): Table = {
      val column = ArrayStrColumn(v.map(_.value).toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CString) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constDate(v: collection.Set[CDate]): Table =  {
      val column = ArrayDateColumn(v.map(_.value).toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CDate) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constArray[A: CValueType](v: collection.Set[CArray[A]]): Table = {
      val column = ArrayHomogeneousArrayColumn(v.map(_.value).toArray(CValueType[A].manifest.arrayManifest))
      Table(Slice(Map(ColumnRef(CPathArray, CArrayType(CValueType[A])) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constNull: Table = 
      Table(Slice(Map(ColumnRef(CPath.Identity, CNull) -> new InfiniteColumn with NullColumn), 1) :: StreamT.empty[M, Slice], ExactSize(1))

    def constEmptyObject: Table = 
      Table(Slice(Map(ColumnRef(CPath.Identity, CEmptyObject) -> new InfiniteColumn with EmptyObjectColumn), 1) :: StreamT.empty[M, Slice], ExactSize(1))

    def constEmptyArray: Table = 
      Table(Slice(Map(ColumnRef(CPath.Identity, CEmptyArray) -> new InfiniteColumn with EmptyArrayColumn), 1) :: StreamT.empty[M, Slice], ExactSize(1))

    def transformStream[A](sliceTransform: SliceTransform1[A], slices: StreamT[M, Slice]): StreamT[M, Slice] = {
      def stream(state: A, slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT(
        for {
          head <- slices.uncons
        } yield {
          head map { case (s, sx) =>
            val (nextState, s0) = sliceTransform.f(state, s)
            StreamT.Yield(s0, stream(nextState, sx))
          } getOrElse {
            StreamT.Done
          }
        }
      )

      stream(sliceTransform.initial, slices)
    }

    /**
     * Intersects the given tables on identity, where identity is defined by the provided TransSpecs
     */
    def intersect(identitySpec: TransSpec1, tables: Table*): M[Table] = {
      val inputCount = tables.size
      val mergedSlices: StreamT[M, Slice] = tables.map(_.slices).reduce( _ ++ _ )
      Table(mergedSlices, UnknownSize).sort(identitySpec).map {
        sortedTable => {
          sealed trait CollapseState
          case class Boundary(prevSlice: Slice, prevStartIdx: Int) extends CollapseState
          case object InitialCollapse extends CollapseState

          def genComparator(sl1: Slice, sl2: Slice) = Slice.rowComparatorFor(sl1, sl2) { slice =>
            // only need to compare identities (field "0" of the sorted table) between projections
            // TODO: Figure out how we might do this directly with the identitySpec
            slice.columns.keys collect { case ColumnRef(path, _) if path.nodes.startsWith(CPathField("0") :: Nil) => path }
          }
          
          def boundaryCollapse(prevSlice: Slice, prevStart: Int, curSlice: Slice): (BitSet, Int) = {
            val comparator = genComparator(prevSlice, curSlice)

            var curIndex = 0

            while (curIndex < curSlice.size && comparator.compare(prevStart, curIndex) == EQ) {
              curIndex += 1
            }

            if (curIndex == 0) {
              // First element is unequal...
              // We either marked the span to retain in the previous slice, or 
              // we don't have enough here to mark the new slice to retain
              (new BitSet, curIndex)
            } else {
              val count = (prevSlice.size - prevStart) + curIndex

              if (count == inputCount) {
                val bs = new BitSet
                bs.set(curIndex - 1)
                (bs, curIndex)
              } else if (count > inputCount) {
                sys.error("Found too many EQ identities in intersect. This indicates a bug in the graph processing algorithm.")
              } else {
                (new BitSet, curIndex)
              }
            }
          } 

          // Collapse the slices, returning the BitSet for which the rows are defined as well as the start of the
          // last span 
          def selfCollapse(slice: Slice, startIndex: Int, defined: BitSet): (BitSet, Int) = {
            val comparator = genComparator(slice, slice)

            var retain = defined

            // We'll collect spans of EQ rows in chunks, retainin the start row of completed spans with the correct
            // count and then inchworming over it
            var spanStart = startIndex
            var spanEnd   = startIndex
            
            while (spanEnd < slice.size) {
              while (spanEnd < slice.size && comparator.compare(spanStart, spanEnd) == EQ) {
                spanEnd += 1
              }

              val count = spanEnd - spanStart

              if (count == inputCount) {
                retain += (spanEnd - 1)
              } else if (count > inputCount) {
                sys.error("Found too many EQ identities in intersect. This indicates a bug in the graph processing algorithm.")
              }

              if (spanEnd < slice.size) {
                spanStart = spanEnd
              }
            }

            (retain, spanStart)
          }

          val collapse = SliceTransform1[CollapseState](InitialCollapse, {
            case (InitialCollapse, slice) => {
              val (retain, spanStart) = selfCollapse(slice, 0, new BitSet)
              // Pass on the remainder, if any, of this slice to the next slice for continued comparison
              (Boundary(slice, spanStart), slice.redefineWith(retain))
            }

            case (Boundary(prevSlice, prevStart), slice) => {
              // First, do a boundary comparison on the previous slice to see if we need to retain lead elements in the new slice
              val (boundaryRetain, boundaryEnd) = boundaryCollapse(prevSlice, prevStart, slice)
              val (retain, spanStart) = selfCollapse(slice, boundaryEnd, boundaryRetain)
              (Boundary(slice, spanStart), slice.redefineWith(retain))
            }
          })

          // Break the idents out into field "0", original data in "1"
          val splitIdentsTransSpec = OuterObjectConcat(WrapObject(identitySpec, "0"), WrapObject(Leaf(Source), "1"))

          Table(transformStream(collapse, sortedTable.transform(splitIdentsTransSpec).compact(Leaf(Source)).slices), UnknownSize).transform(DerefObjectStatic(Leaf(Source), CPathField("1")))
        }
      }
    }

    /**
     * Merge controls the iteration over the table of group key values. 
     */
    def merge(grouping: GroupingSpec)(body: (Table, GroupId => M[Table]) => M[Table]): M[Table] = {
      import GroupKeySpec.{ dnf, toVector }
      
      type Key = Seq[JValue]
      type KeySchema = Seq[CPathField]
      
      def sources(spec: GroupKeySpec): Seq[GroupKeySpecSource] = (spec: @unchecked) match {
        case GroupKeySpecAnd(left, right) => sources(left) ++ sources(right)
        case src: GroupKeySpecSource => Vector(src)
      }

      def mkProjections(spec: GroupKeySpec) =
        toVector(dnf(spec)).map(sources(_).map { s => (s.key, s.spec) })
      
      case class IndexedSource(groupId: GroupId, index: TableIndex, keySchema: KeySchema) 
        
      (for {
        source <- grouping.sources
        groupKeyProjections <- mkProjections(source.groupKeySpec)
        disjunctGroupKeyTransSpecs = groupKeyProjections.map { case (key, spec) => spec }
      } yield {
        TableIndex.createFromTable(source.table, disjunctGroupKeyTransSpecs, source.targetTrans.getOrElse(TransSpec1.Id)).map { index =>
          IndexedSource(source.groupId, index, groupKeyProjections.map(_._1))
        }
      }).sequence.flatMap { sourceKeys =>
        val fullSchema = sourceKeys.flatMap(_.keySchema).distinct
        
        val indicesGroupedBySource = sourceKeys.groupBy(_.groupId).mapValues(_.map(y => (y.index, y.keySchema)).toSeq).values.toSeq
        
        def unionOfIntersections(indicesGroupedBySource: Seq[Seq[(TableIndex, KeySchema)]]) = {
          def allSourceDNF[T](l : Seq[Seq[T]]): Seq[Seq[T]] = {
            l match {
              case Seq(hd) => hd.map(Seq(_))
              case Seq(hd, tl @ _*) => {
                for {
                  disjunctHd <- hd
                  disjunctTl <- allSourceDNF(tl)
                } yield disjunctHd +: disjunctTl
              }
              case empty => empty
            }
          }
          
          def normalizedKeys(index: TableIndex, keySchema: KeySchema): collection.Set[Key] = {
            val schemaMap = for(k <- fullSchema) yield keySchema.indexOf(k)
            for(key <- index.getUniqueKeys)
              yield for(k <- schemaMap) yield if (k == -1) JUndefined else key(k) 
          }

          def intersect(keys0: collection.Set[Key], keys1: collection.Set[Key]): collection.Set[Key] = {
            def consistent(key0: Key, key1: Key): Boolean =
              (key0 zip key1).forall {
                case (k0, k1) => k0 == k1 || k0 == JUndefined || k1 == JUndefined 
              }
            
            def merge(key0: Key, key1: Key): Key =
              (key0 zip key1).map {
                case (k0, JUndefined) => k0 
                case (_,  k1        ) => k1 
              }
  
            // TODO: This "mini-cross" is much better than the
            // previous mega-cross. However in many situations we
            // could do even less work. Consider further optimization
            // (e.g. when one key schema is a subset of the other).

            // Relatedly it might make sense to topologically sort the
            // Indices by their keyschemas so that we end up intersecting
            // key with their subset.
            keys0.flatMap { key0 =>
              keys1.flatMap(key1 => if(consistent(key0, key1)) Some(merge(key0, key1)) else None)
            }
          }

          allSourceDNF(indicesGroupedBySource).foldLeft(Set.empty[Key]) {
            case (acc, intersection) =>
              val hd = normalizedKeys(intersection.head._1, intersection.head._2)
              acc | intersection.tail.foldLeft(hd) {
                case (keys0, (index1, schema1)) =>
                  val keys1 = normalizedKeys(index1, schema1)
                  intersect(keys0, keys1)
              }
          }
        }

        def tableFromGroupKey(key: Seq[JValue], cpaths: Seq[CPathField]): Table = {
          val items = (cpaths zip key).map(t => (t._1.name, t._2))
          val row = JObject(items.toMap)
          Table.fromJson(row #:: Stream.empty)
        }

        val groupKeys = unionOfIntersections(indicesGroupedBySource)

        val evaluatorResults = for (groupKey <- groupKeys) yield {
          val groupKeyTable = tableFromGroupKey(groupKey, fullSchema)

          def map(gid: GroupId): M[Table] = {
            val subTableProjections = (sourceKeys.filter(_.groupId == gid).map { indexedSource =>
              val keySchema = indexedSource.keySchema
              val projectedKeyIndices = for(k <- fullSchema) yield keySchema.indexOf(k)
              (indexedSource.index, projectedKeyIndices, groupKey)
            }).toList

            // TODO: filter empty slices earlier maybe?
            val t = TableIndex.joinSubTables(subTableProjections).normalize
            t.sort(DerefObjectStatic(Leaf(Source), CPathField("key")))
          }

          body(groupKeyTable, map)
        }

        // TODO: consider moving to TableCompanion.concat?
        evaluatorResults.sequence.map { tables =>
          val (slices, size) = tables.foldLeft((StreamT.empty[M, Slice], TableSize(0))) {
            case ((slices, size), table) =>
              (slices ++ table.slices, size + table.size)
          }
          Table(slices, size)
        }
      }
    }

    /// Utility Methods ///

    /**
     * Reduce the specified table to obtain the in-memory set of strings representing the vfs paths
     * to be loaded.
     */
    protected def pathsM(table: Table) = {
      table reduce {
        new CReducer[Set[Path]] {
          def reduce(columns: JType => Set[Column], range: Range): Set[Path] = {
            columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _ => Set()
            }
          }
        }
      }
    }

    /**
     * Given a JValue, an existing map of columnrefs to column data,
     * a sliceIndex, and a sliceSize, return an updated map.
     */
    def withIdsAndValues(jv: JValue, into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int, sliceSize: Int, remapPath: Option[JPath => CPath] = None): Map[ColumnRef, (BitSet, Array[_])] = {

      jv.flattenWithPath.foldLeft(into) {
        case (acc, (jpath, JUndefined)) => acc
        case (acc, (jpath, v)) =>
          val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
          val ref = ColumnRef(remapPath.map(_(jpath)).getOrElse(CPath(jpath)), ctype)
          
          val pair: (BitSet, Array[_]) = v match {
            case JBool(b) =>
              val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[Boolean](sliceSize))).asInstanceOf[(BitSet, Array[Boolean])]
              col(sliceIndex) = b
              (defined + sliceIndex, col)
              
            case JNum(d) => {
              val isLong = ctype == CLong
              val isDouble = ctype == CDouble
              
              val (defined, col) = if (isLong) {
                val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[Long](sliceSize))).asInstanceOf[(BitSet, Array[Long])]
                col(sliceIndex) = d.toLong
                (defined, col)
              } else if (isDouble) {
                val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[Double](sliceSize))).asInstanceOf[(BitSet, Array[Double])]
                col(sliceIndex) = d.toDouble
                (defined, col)
              } else {
                val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[BigDecimal](sliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                col(sliceIndex) = d
                (defined, col)
              }
              
              (defined + sliceIndex, col)
            }
              
            case JString(s) =>
              val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[String](sliceSize))).asInstanceOf[(BitSet, Array[String])]
              col(sliceIndex) = s
              (defined + sliceIndex, col)
              
            case JArray(Nil) =>
              val (defined, col) = acc.getOrElse(ref, (new BitSet, null)).asInstanceOf[(BitSet, Array[Boolean])]
              (defined + sliceIndex, col)
              
            case JObject(_) =>
              val (defined, col) = acc.getOrElse(ref, (new BitSet, null)).asInstanceOf[(BitSet, Array[Boolean])]
              (defined + sliceIndex, col)
              
            case JNull        =>
              val (defined, col) = acc.getOrElse(ref, (new BitSet, null)).asInstanceOf[(BitSet, Array[Boolean])]
              (defined + sliceIndex, col)
          }
          
          acc + (ref -> pair)
      }
    }

    def fromJson(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table = {
      val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)
  
      def makeSlice(sampleData: Stream[JValue]): (Slice, Stream[JValue]) = {
        val (prefix, suffix) = sampleData.splitAt(sliceSize)
  
        @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int): (Map[ColumnRef, (BitSet, Object)], Int) = {
          from match {
            case jv #:: xs =>
              val refs = withIdsAndValues(jv, into, sliceIndex, sliceSize)
              buildColArrays(xs, refs, sliceIndex + 1)
            case _ =>
              (into, sliceIndex)
          }
        }
    
        // FIXME: If prefix is empty (eg. because sampleData.data is empty) the generated
        // columns won't satisfy sampleData.schema. This will cause the subsumption test in
        // Slice#typed to fail unless it allows for vacuous success
        val slice = new Slice {
          val (cols, size) = buildColArrays(prefix.toStream, Map.empty[ColumnRef, (BitSet, Array[_])], 0) 
          val columns = cols map {
            case (ref @ ColumnRef(_, CBoolean), (defined, values))     => (ref, ArrayBoolColumn(defined, values.asInstanceOf[Array[Boolean]]))
            case (ref @ ColumnRef(_, CLong), (defined, values))        => (ref, ArrayLongColumn(defined, values.asInstanceOf[Array[Long]]))
            case (ref @ ColumnRef(_, CDouble), (defined, values))      => (ref, ArrayDoubleColumn(defined, values.asInstanceOf[Array[Double]]))
            case (ref @ ColumnRef(_, CNum), (defined, values))         => (ref, ArrayNumColumn(defined, values.asInstanceOf[Array[BigDecimal]]))
            case (ref @ ColumnRef(_, CString), (defined, values))      => (ref, ArrayStrColumn(defined, values.asInstanceOf[Array[String]]))
            case (ref @ ColumnRef(_, CEmptyArray), (defined, values))  => (ref, new BitsetColumn(defined) with EmptyArrayColumn)
            case (ref @ ColumnRef(_, CEmptyObject), (defined, values)) => (ref, new BitsetColumn(defined) with EmptyObjectColumn)
            case (ref @ ColumnRef(_, CNull), (defined, values))        => (ref, new BitsetColumn(defined) with NullColumn)
          }
        }
    
        (slice, suffix)
      }
      
      Table(
        StreamT.unfoldM(values) { events =>
          M.point {
            (!events.isEmpty) option {
              makeSlice(events.toStream)
            }
          }
        },
        ExactSize(values.length)
      )
    }
  }

  abstract class ColumnarTable(slices0: StreamT[M, Slice], val size: TableSize) extends TableLike with SamplableColumnarTable { self: Table =>
    import SliceTransform._

    private final val readStarts = new java.util.concurrent.atomic.AtomicInteger
    private final val blockReads = new java.util.concurrent.atomic.AtomicInteger

    val slices = StreamT(
      StreamT.Skip({
        readStarts.getAndIncrement
        slices0.map(s => { blockReads.getAndIncrement; s })
      }).point[M]
    )

    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */

    def reduce[A](reducer: Reducer[A])(implicit monoid: Monoid[A]): M[A] = {  
      def rec(stream: StreamT[M, A], acc: A): M[A] = {
        stream.uncons flatMap {
          case Some((head, tail)) => rec(tail, head |+| acc) 
          case None => M.point(acc)
        }
      }

      rec(slices map { s => reducer.reduce(s.logicalColumns, 0 until s.size) }, monoid.zero)
    }

    def compact(spec: TransSpec1): Table = {
      val specTransform = SliceTransform.composeSliceTransform(spec)
      val compactTransform = SliceTransform.composeSliceTransform(Leaf(Source)).zip(specTransform) { (s1, s2) => s1.compact(s2, AnyDefined) }
      Table(Table.transformStream(compactTransform, slices), size).normalize
    }

    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TransSpec1): Table = {
      Table(Table.transformStream(composeSliceTransform(spec), slices), this.size)
    }
    
    def force: M[Table] = this.sort(Scan(Leaf(Source), freshIdScanner), SortAscending) //, unique = true)
    
    def paged(limit: Int): Table = {
      val slices2 = slices flatMap { slice =>
        StreamT.unfoldM(0) { idx =>
          val back = if (idx >= slice.size)
            None
          else
            Some((slice.takeRange(idx, limit), idx + limit))
          
          M.point(back)
        }
      }
      
      Table(slices2, size)
    }

    def concat(t2: Table): Table = {
      val resultSize = TableSize(size.maxSize + t2.size.maxSize)
      val resultSlices = slices ++ t2.slices
      Table(resultSlices, resultSize)
    }

    def toArray[A](implicit tpe: CValueType[A]): Table = {
      val slices2 = slices map { _.toArray[A] }
      Table(slices2, size)
    }

    /**
     * Cogroups this table with another table, using equality on the specified
     * transformation on rows of the table.
     */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table = {

      //println("Cogrouping with respect to\nleftKey: " + leftKey + "\nrightKey: " + rightKey)
      class IndexBuffers(lInitialSize: Int, rInitialSize: Int) {
        val lbuf = new ArrayIntList(lInitialSize)
        val rbuf = new ArrayIntList(rInitialSize)
        val leqbuf = new ArrayIntList(lInitialSize max rInitialSize)
        val reqbuf = new ArrayIntList(lInitialSize max rInitialSize)

        @inline def advanceLeft(lpos: Int): Unit = {
          lbuf.add(lpos)
          rbuf.add(-1)
          leqbuf.add(-1)
          reqbuf.add(-1)
        }

        @inline def advanceRight(rpos: Int): Unit = {
          lbuf.add(-1)
          rbuf.add(rpos)
          leqbuf.add(-1)
          reqbuf.add(-1)
        }

        @inline def advanceBoth(lpos: Int, rpos: Int): Unit = {
          lbuf.add(-1)
          rbuf.add(-1)
          leqbuf.add(lpos)
          reqbuf.add(rpos)
        }

        def cogrouped[LR, RR, BR](lslice: Slice, 
                                  rslice: Slice, 
                                  leftTransform:  SliceTransform1[LR], 
                                  rightTransform: SliceTransform1[RR], 
                                  bothTransform:  SliceTransform2[BR]): (Slice, LR, RR, BR) = {

          val remappedLeft = lslice.remap(lbuf)
          val remappedRight = rslice.remap(rbuf)

          val remappedLeq = lslice.remap(leqbuf)
          val remappedReq = rslice.remap(reqbuf)

          val (ls0, lx) = leftTransform(remappedLeft)
          val (rs0, rx) = rightTransform(remappedRight)
          val (bs0, bx) = bothTransform(remappedLeq, remappedReq)

          assert(lx.size == rx.size && rx.size == bx.size)
          val resultSlice = lx zip rx zip bx

          (resultSlice, ls0, rs0, bs0)
        }

        override def toString = {
          "left: " + lbuf.toArray.mkString("[", ",", "]") + "\n" + 
          "right: " + rbuf.toArray.mkString("[", ",", "]") + "\n" + 
          "both: " + (leqbuf.toArray zip reqbuf.toArray).mkString("[", ",", "]")
        }
      }

      case class SlicePosition[K](
        /** The position in the current slice. This will only be nonzero when the slice has been appended
         * to as a result of a cartesian crossing the slice boundary */
        pos: Int,
        /** Present if not in a final right or left run. A pair of a key slice that is parallel to the
         * current data slice, and the value that is needed as input to sltk or srtk to produce the next key. */
        keyState: K,
        key: Slice,
        /** The current slice to be operated upon. */
        data: Slice,
        /** The remainder of the stream to be operated upon. */
        tail: StreamT[M, Slice])

      sealed trait NextStep[A, B]
      case class SplitLeft[A, B](lpos: Int) extends NextStep[A, B]
      case class SplitRight[A, B](rpos: Int) extends NextStep[A, B]
      case class NextCartesianLeft[A, B](left: SlicePosition[A], right: SlicePosition[B], rightStart: Option[SlicePosition[B]], rightEnd: Option[SlicePosition[B]]) extends NextStep[A, B]
      case class NextCartesianRight[A, B](left: SlicePosition[A], right: SlicePosition[B], rightStart: Option[SlicePosition[B]], rightEnd: Option[SlicePosition[B]]) extends NextStep[A, B]
      case class SkipRight[A, B](left: SlicePosition[A], rightEnd: SlicePosition[B]) extends NextStep[A, B]
      case class RestartRight[A, B](left: SlicePosition[A], rightStart: SlicePosition[B], rightEnd: SlicePosition[B]) extends NextStep[A, B]
      def cogroup0[LK, RK, LR, RR, BR](stlk: SliceTransform1[LK], strk: SliceTransform1[RK], stlr: SliceTransform1[LR], strr: SliceTransform1[RR], stbr: SliceTransform2[BR]) = {

        sealed trait CogroupState
        case class EndLeft(lr: LR, lhead: Slice, ltail: StreamT[M, Slice]) extends CogroupState
        case class Cogroup(lr: LR, rr: RR, br: BR, left: SlicePosition[LK], right: SlicePosition[RK], rightStart: Option[SlicePosition[RK]], rightEnd: Option[SlicePosition[RK]]) extends CogroupState
        case class EndRight(rr: RR, rhead: Slice, rtail: StreamT[M, Slice]) extends CogroupState
        case object CogroupDone extends CogroupState

        val Reset = -1
        
        // step is the continuation function fed to uncons. It is called once for each emitted slice
        def step(state: CogroupState): M[Option[(Slice, CogroupState)]] = {

          // step0 is the inner monadic recursion needed to cross slice boundaries within the emission of a slice
          def step0(lr: LR, rr: RR, br: BR, leftPosition: SlicePosition[LK], rightPosition: SlicePosition[RK], rightStart0: Option[SlicePosition[RK]], rightEnd0: Option[SlicePosition[RK]])
                   (ibufs: IndexBuffers = new IndexBuffers(leftPosition.key.size, rightPosition.key.size)): M[Option[(Slice, CogroupState)]] = {

            val SlicePosition(lpos0, lkstate, lkey, lhead, ltail) = leftPosition
            val SlicePosition(rpos0, rkstate, rkey, rhead, rtail) = rightPosition

            val comparator = Slice.rowComparatorFor(lkey, rkey) {
              // since we've used the key transforms, and since transforms are contracturally
              // forbidden from changing slice size, we can just use all
              _.columns.keys map (_.selector)
            }

            // the inner tight loop; this will recur while we're within the bounds of
            // a pair of slices. Any operation that must cross slice boundaries
            // must exit this inner loop and recur through the outer monadic loop
            // xrstart is an int with sentinel value for effieiency, but is Option at the slice level.
            @inline @tailrec def buildRemappings(lpos: Int, rpos: Int, rightStart: Option[SlicePosition[RK]], rightEnd: Option[SlicePosition[RK]], endRight: Boolean): NextStep[LK, RK] = {
              //println("lpos = %d, rpos = %d, rightStart = %s, rightEnd = %s, endRight = %s" format (lpos, rpos, rightStart, rightEnd, endRight))
              //println("Left key: " + lkey.toJson(lpos))
              //println("Right key: " + rkey.toJson(rpos))
              //println("Left data: " + lhead.toJson(lpos))
              //println("Right data: " + rhead.toJson(rpos))

              rightStart match {
                case Some(resetMarker @ SlicePosition(rightStartPos, _, rightStartSlice, _, _)) =>
                  // We're currently in a cartesian.
                  if (lpos < lhead.size && rpos < rhead.size) {
                    comparator.compare(lpos, rpos) match {
                      case LT if rightStartSlice == rkey =>
                        buildRemappings(lpos + 1, rightStartPos, rightStart, Some(rightPosition.copy(pos = rpos)), endRight)
                      case LT =>
                        // Transition to emit the current slice and reset the right side, carry rightPosition through
                        RestartRight(leftPosition.copy(pos = lpos + 1), resetMarker, rightPosition.copy(pos = rpos))
                      case GT =>
                        // catch input-out-of-order errors early
                        rightEnd match {
                          case None =>
                            //println("lhead\n" + lhead.toJsonString())
                            //println("rhead\n" + rhead.toJsonString())
                            sys.error("Inputs are not sorted; value on the left exceeded value on the right at the end of equal span. lpos = %d, rpos = %d".format(lpos, rpos))

                          case Some(SlicePosition(endPos, _, endSlice, _, _)) if endSlice == rkey =>
                            buildRemappings(lpos, endPos, None, None, endRight)

                          case Some(rend @ SlicePosition(_, _, _, _, _)) =>
                            // Step out of buildRemappings so that we can restart with the current rightEnd
                            SkipRight(leftPosition.copy(pos = lpos), rend)
                        }
                      case EQ =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, rightStart, rightEnd, endRight)
                    }
                  } else if (lpos < lhead.size) {
                    if (rightStartSlice == rkey) {
                      // we know there won't be another slice on the RHS, so just keep going to exhaust the left
                      buildRemappings(lpos + 1, rightStartPos, rightStart, Some(rightPosition.copy(pos = rpos)), endRight)
                    } else if (endRight) {
                      RestartRight(leftPosition.copy(pos = lpos + 1), resetMarker, rightPosition.copy(pos = rpos))
                    } else {
                      // right slice is exhausted, so we need to emit that slice from the right tail
                      // then continue in the cartesian
                      NextCartesianRight(leftPosition.copy(pos = lpos + 1), rightPosition, rightStart, rightEnd)
                    }
                  } else if (rpos < rhead.size) {
                    // left slice is exhausted, so we need to emit that slice from the left tail
                    // then continue in the cartesian
                    NextCartesianLeft(leftPosition, rightPosition.copy(pos = rpos), rightStart, rightEnd)
                  } else {
                    sys.error("This state should be unreachable, since we only increment one side at a time.")
                  }

                case None =>
                  // not currently in a cartesian, hence we can simply proceed.
                  if (lpos < lhead.size && rpos < rhead.size) {
                    comparator.compare(lpos, rpos) match {
                      case LT =>
                        ibufs.advanceLeft(lpos)
                        buildRemappings(lpos + 1, rpos, None, None, endRight)
                      case GT =>
                        ibufs.advanceRight(rpos)
                        buildRemappings(lpos, rpos + 1, None, None, endRight)
                      case EQ =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, Some(rightPosition.copy(pos = rpos)), None, endRight)
                    }
                  } else if (lpos < lhead.size) {
                    // right side is exhausted, so we should just split the left and emit
                    SplitLeft(lpos)
                  } else if (rpos < rhead.size) {
                    // left side is exhausted, so we should just split the right and emit
                    SplitRight(rpos)
                  } else {
                    sys.error("This state should be unreachable, since we only increment one side at a time.")
                  }
              }
            }

            def continue(nextStep: NextStep[LK, RK]): M[Option[(Slice, CogroupState)]] = nextStep match {
              case SplitLeft(lpos) =>

                val (lpref, lsuf) = lhead.split(lpos)
                val (_, lksuf) = lkey.split(lpos)
                val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(lpref, rhead, 
                                                                     SliceTransform1[LR](lr, stlr.f),
                                                                     SliceTransform1[RR](rr, strr.f),
                                                                     SliceTransform2[BR](br, stbr.f))

                rtail.uncons map {
                  case Some((nextRightHead, nextRightTail)) => 
                    val (rkstate0, rkey0) = strk.f(rkstate, nextRightHead)
                    val nextState = Cogroup(lr0, rr0, br0, 
                                            SlicePosition(0, lkstate,  lksuf, lsuf, ltail),
                                            SlicePosition(0, rkstate0, rkey0, nextRightHead, nextRightTail),
                                            None, None)

                    Some(completeSlice -> nextState)

                  case None => 
                    val nextState = EndLeft(lr0, lsuf, ltail)
                    Some(completeSlice -> nextState)
                }

              case SplitRight(rpos) => 

                val (rpref, rsuf) = rhead.split(rpos)
                val (_, rksuf) = rkey.split(rpos)
                val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(lhead, rpref, 
                                                                     SliceTransform1[LR](lr, stlr.f),
                                                                     SliceTransform1[RR](rr, strr.f),
                                                                     SliceTransform2[BR](br, stbr.f))


                ltail.uncons map {
                  case Some((nextLeftHead, nextLeftTail)) =>
                    val (lkstate0, lkey0) = stlk.f(lkstate, nextLeftHead)
                    val nextState = Cogroup(lr0, rr0, br0,
                                            SlicePosition(0, lkstate0, lkey0, nextLeftHead, nextLeftTail),
                                            SlicePosition(0, rkstate,  rksuf, rsuf, rtail),
                                            None, None)

                    Some(completeSlice -> nextState)

                  case None =>
                    val nextState = EndRight(rr0, rsuf, rtail)
                    Some(completeSlice -> nextState)
                }

              case NextCartesianLeft(left, right, rightStart, rightEnd) =>
                left.tail.uncons.map {
                  case Some((nextLeftHead, nextLeftTail)) =>
                    val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(left.data, right.data,
                                                                         SliceTransform1[LR](lr, stlr.f),
                                                                         SliceTransform1[RR](rr, strr.f),
                                                                         SliceTransform2[BR](br, stbr.f))

                    val (lkstate0, lkey0) = stlk.f(lkstate, nextLeftHead)
                    val nextState = Cogroup(lr0, rr0, br0,
                          SlicePosition(0, lkstate0, lkey0, nextLeftHead, nextLeftTail),
                          right,
                          rightStart, rightEnd)

                    Some(completeSlice -> nextState)

                  case None =>
                    (rightStart, rightEnd) match {
                      case (Some(_), Some(end)) =>
                        val (rpref, rsuf) = right.data.split(end.pos)
                        val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(left.data, rpref,
                                                                             SliceTransform1[LR](lr, stlr.f),
                                                                             SliceTransform1[RR](rr, strr.f),
                                                                             SliceTransform2[BR](br, stbr.f))

                        val nextState = EndRight(rr0, rsuf, right.tail)
                        Some(completeSlice -> nextState)

                      case _ =>
                        val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(left.data, right.data,
                                                                             SliceTransform1[LR](lr, stlr.f),
                                                                             SliceTransform1[RR](rr, strr.f),
                                                                             SliceTransform2[BR](br, stbr.f))

                        Some(completeSlice -> CogroupDone)
                    }
                }

              case NextCartesianRight(left, right, rightStart, rightEnd) =>
                right.tail.uncons.flatMap {
                  case Some((nextRightHead, nextRightTail)) =>
                    val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(left.data, right.data,
                                                                         SliceTransform1[LR](lr, stlr.f),
                                                                         SliceTransform1[RR](rr, strr.f),
                                                                         SliceTransform2[BR](br, stbr.f))

                    val (rkstate0, rkey0) = strk.f(rkstate, nextRightHead)
                    val nextState = Cogroup(lr0, rr0, br0,
                          left,
                          SlicePosition(0, rkstate0, rkey0, nextRightHead, nextRightTail),
                          rightStart, rightEnd)

                    M.point(Some(completeSlice -> nextState))

                  case None =>
                    continue(buildRemappings(left.pos, right.pos, rightStart, rightEnd, true))
                }

              case SkipRight(left, rightEnd) =>
                continue(buildRemappings(left.pos, rightPosition.pos, rightStart0, Some(rightEnd), true))

              case RestartRight(left, rightStart, rightEnd) =>
                val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(left.data, rightPosition.data,
                                                                     SliceTransform1[LR](lr, stlr.f),
                                                                     SliceTransform1[RR](rr, strr.f),
                                                                     SliceTransform2[BR](br, stbr.f))

                val nextState = Cogroup(lr0, rr0, br0,
                                        left,
                                        rightPosition,
                                        Some(rightStart), Some(rightEnd))

                M.point(Some(completeSlice -> nextState))
            }

            continue(buildRemappings(lpos0, rpos0, rightStart0, rightEnd0, false))
          } // end of step0 

          state match {
            case EndLeft(lr, data, tail) =>
              val (lr0, leftResult) = stlr.f(lr, data)
              tail.uncons map { unconsed =>
                Some(leftResult -> (unconsed map { case (nhead, ntail) => EndLeft(lr0, nhead, ntail) } getOrElse CogroupDone))
              }

            case Cogroup(lr, rr, br, left, right, rightReset, rightEnd) =>
              step0(lr, rr, br, left, right, rightReset, rightEnd)()

            case EndRight(rr, data, tail) =>
              val (rr0, rightResult) = strr.f(rr, data)
              tail.uncons map { unconsed =>
                Some(rightResult -> (unconsed map { case (nhead, ntail) => EndRight(rr0, nhead, ntail) } getOrElse CogroupDone))
              }

            case CogroupDone => M.point(None)
          }
        } // end of step

        val initialState = for {
          // We have to compact both sides to avoid any rows for which the key is completely undefined
          leftUnconsed  <- self.compact(leftKey).slices.uncons
          rightUnconsed <- that.compact(rightKey).slices.uncons
        } yield {
          val cogroup = for {
            (leftHead, leftTail)   <- leftUnconsed
            (rightHead, rightTail) <- rightUnconsed
          } yield {
            val (lkstate, lkey) = stlk(leftHead)
            val (rkstate, rkey) = strk(rightHead)
            Cogroup(stlr.initial, strr.initial, stbr.initial, 
                    SlicePosition(0, lkstate, lkey, leftHead,  leftTail), 
                    SlicePosition(0, rkstate, rkey, rightHead, rightTail),
                    None, None)
          } 

          cogroup orElse {
            leftUnconsed map {
              case (head, tail) => EndLeft(stlr.initial, head, tail)
            }
          } orElse {
            rightUnconsed map {
              case (head, tail) => EndRight(strr.initial, head, tail)
            }
          }
        }

        Table(StreamT.wrapEffect(initialState map { state => StreamT.unfoldM[M, Slice, CogroupState](state getOrElse CogroupDone)(step) }), UnknownSize)
      }

      cogroup0(composeSliceTransform(leftKey),
               composeSliceTransform(rightKey),
               composeSliceTransform(leftResultTrans),
               composeSliceTransform(rightResultTrans),
               composeSliceTransform2(bothResultTrans))
    }

    /**
     * Performs a full cartesian cross on this table with the specified table,
     * applying the specified transformation to merge the two tables into
     * a single table.
     */
    def cross(that: Table)(spec: TransSpec2): Table = {
      def cross0[A](transform: SliceTransform2[A]): M[StreamT[M, Slice]] = {
        case class CrossState(a: A, position: Int, tail: StreamT[M, Slice])

        def crossLeftSingle(lhead: Slice, right: StreamT[M, Slice]): StreamT[M, Slice] = {
          def step(state: CrossState): M[Option[(Slice, CrossState)]] = {
            if (state.position < lhead.size) {
              state.tail.uncons flatMap {
                case Some((rhead, rtail0)) =>
                  val lslice = new Slice {
                    val size = rhead.size
                    val columns = lhead.columns.lazyMapValues(Remap(i => state.position)(_).get)
                  }

                  val (a0, resultSlice) = transform.f(state.a, lslice, rhead)
                  M.point(Some((resultSlice, CrossState(a0, state.position, rtail0))))
                  
                case None => 
                  step(CrossState(state.a, state.position + 1, right))
              }
            } else {
              M.point(None)
            }
          }

          StreamT.unfoldM(CrossState(transform.initial, 0, right))(step _)
        }
        
        def crossRightSingle(left: StreamT[M, Slice], rhead: Slice): StreamT[M, Slice] = {
          def step(state: CrossState): M[Option[(Slice, CrossState)]] = {
            state.tail.uncons map {
              case Some((lhead, ltail0)) =>
                val lslice = new Slice {
                  val size = rhead.size * lhead.size
                  val columns = if (rhead.size == 0)
                    lhead.columns.lazyMapValues(Empty(_).get)
                  else
                    lhead.columns.lazyMapValues(Remap(_ / rhead.size)(_).get)
                }

                val rslice = new Slice {
                  val size = rhead.size * lhead.size
                  val columns = if (rhead.size == 0)
                    rhead.columns.lazyMapValues(Empty(_).get)
                  else
                    rhead.columns.lazyMapValues(Remap(_ % rhead.size)(_).get)
                }

                val (a0, resultSlice) = transform.f(state.a, lslice, rslice)
                Some((resultSlice, CrossState(a0, state.position, ltail0)))
                
              case None => None
            }
          }

          StreamT.unfoldM(CrossState(transform.initial, 0, left))(step _)
        }

        def crossBoth(ltail: StreamT[M, Slice], rtail: StreamT[M, Slice]): StreamT[M, Slice] = {
          ltail.flatMap(crossLeftSingle(_ :Slice, rtail))
        }

        this.slices.uncons flatMap {
          case Some((lhead, ltail)) =>
            that.slices.uncons flatMap {
              case Some((rhead, rtail)) =>
                for {
                  lempty <- ltail.isEmpty //TODO: Scalaz result here is negated from what it should be!
                  rempty <- rtail.isEmpty
                } yield {
                  val frontSize = lhead.size * rhead.size
                  
                  if (lempty && frontSize <= yggConfig.maxSliceSize) {
                    // left side is a small set, so restart it in memory
                    crossLeftSingle(lhead, rhead :: rtail)
                  } else if (rempty && frontSize <= yggConfig.maxSliceSize) {
                    // right side is a small set, so restart it in memory
                    crossRightSingle(lhead :: ltail, rhead)
                  } else {
                    // both large sets, so just walk the left restarting the right.
                    crossBoth(this.slices, that.slices)
                  }
                }

              case None => M.point(StreamT.empty[M, Slice])
            }

          case None => M.point(StreamT.empty[M, Slice])
        }
      }
      
      // TODO: We should be able to fully compute the size of the result above.
      val newSize = (size, that.size) match {
        case (ExactSize(l), ExactSize(r))         => TableSize(l max r, l * r)
        case (EstimateSize(ln, lx), ExactSize(r)) => TableSize(ln max r, lx * r)
        case (ExactSize(l), EstimateSize(rn, rx)) => TableSize(l max rn, l * rx)
        case _ => UnknownSize // Bail on anything else for now (see above TODO)
      }
      
      val newSizeM = newSize match {
        case ExactSize(s) => Some(s)
        case EstimateSize(_, s) => Some(s)
        case _ => None
      }

      val sizeCheck = for (resultSize <- newSizeM) yield
        resultSize < yggConfig.maxSaneCrossSize && resultSize >= 0

      if (sizeCheck getOrElse true)
        Table(StreamT(cross0(composeSliceTransform2(spec)) map { tail => StreamT.Skip(tail) }), newSize)
      else
        throw EnormousCartesianException(this.size, that.size)
    }
    
    /**
     * Yields a new table with distinct rows. Assumes this table is sorted.
     */
    def distinct(spec: TransSpec1): Table = {
      def distinct0[T](id: SliceTransform1[Option[Slice]], filter: SliceTransform1[T]): Table = {
        def stream(state: (Option[Slice], T), slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT(
          for {
            head <- slices.uncons
          } yield
            head map { case (s, sx) =>
              val (prevFilter, cur) = id.f(state._1, s)
              val (nextT, curFilter) = filter.f(state._2, s)
              
              val next = cur.distinct(prevFilter, curFilter)
              
              StreamT.Yield(next, stream((if (next.size > 0) Some(curFilter) else prevFilter, nextT), sx))
            } getOrElse {
              StreamT.Done
            }
        )
        
        Table(stream((id.initial, filter.initial), slices), UnknownSize)
      }

      distinct0(SliceTransform.identity(None : Option[Slice]), composeSliceTransform(spec))
    }
    
    def takeRange(startIndex: Long, numberToTake: Long): Table = {
      def loop(stream: StreamT[M, Slice], readSoFar: Long): M[StreamT[M, Slice]] = stream.uncons flatMap {
        // Prior to first needed slice, so skip
        case Some((head, tail)) if (readSoFar + head.size) < (startIndex + 1) => loop(tail, readSoFar + head.size)
        // Somewhere in between, need to transition to splitting/reading
        case Some(_) if readSoFar < (startIndex + 1) => inner(stream, 0, (startIndex - readSoFar).toInt)
        // Read off the end (we took nothing)
        case _ => M.point(StreamT.empty[M, Slice])
      }
          
      def inner(stream: StreamT[M, Slice], takenSoFar: Long, sliceStartIndex: Int): M[StreamT[M, Slice]] = stream.uncons flatMap {
        case Some((head, tail)) if takenSoFar < numberToTake => {
          val needed = head.takeRange(sliceStartIndex, (numberToTake - takenSoFar).toInt)
          inner(tail, takenSoFar + (head.size - (sliceStartIndex)), 0).map(needed :: _)
        }
        case _ => M.point(StreamT.empty[M, Slice])
      }
      
      def calcNewSize(current: Long): Long = ((current-startIndex) max 0) min numberToTake

      val newSize = size match {
        case ExactSize(sz) => ExactSize(calcNewSize(sz))
        case EstimateSize(sMin, sMax) => TableSize(calcNewSize(sMin), calcNewSize(sMax))
        case UnknownSize => UnknownSize
      }

      Table(StreamT.wrapEffect(loop(slices, 0)), newSize)
    }

    /**
     * In order to call partitionMerge, the table must be sorted according to 
     * the values specified by the partitionBy transspec.
     */
    def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table] = {
      // Find the first element that compares LT
      @tailrec def findEnd(compare: Int => Ordering, imin: Int, imax: Int): Int = {
        val minOrd = compare(imin)
        if (minOrd eq EQ) {
          val maxOrd = compare(imax) 
          if (maxOrd eq EQ) {
            imax + 1
          } else if (maxOrd eq LT) {
            val imid = imin + ((imax - imin) / 2)
            val midOrd = compare(imid)
            if (midOrd eq LT) {
              findEnd(compare, imin, imid - 1)
            } else if (midOrd eq EQ) {
              findEnd(compare, imid, imax - 1)
            } else {
              sys.error("Inputs to partitionMerge not sorted.")
            }
          } else {
            sys.error("Inputs to partitionMerge not sorted.")
          }
        } else if ((minOrd eq LT) && (compare(imax) eq LT)) {
          imin
        } else {
          sys.error("Inputs to partitionMerge not sorted.")
        }
      }

      def subTable(comparatorGen: Slice => (Int => Ordering), slices: StreamT[M, Slice]): M[Table] = {
        def subTable0(slices: StreamT[M, Slice], subSlices: StreamT[M, Slice], size: Int): M[Table] = {
          slices.uncons flatMap {
            case Some((head, tail)) =>
              val headComparator = comparatorGen(head)
              val spanEnd = findEnd(headComparator, 0, head.size - 1)
              if (spanEnd < head.size) {
                M.point(Table(subSlices ++ (head.take(spanEnd) :: StreamT.empty[M, Slice]), ExactSize(size+spanEnd)))
              } else {
                subTable0(tail, subSlices ++ (head :: StreamT.empty[M, Slice]), size+head.size)
              }
              
            case None =>
              M.point(Table(subSlices, ExactSize(size)))
          }
        }
        
        subTable0(slices, StreamT.empty[M, Slice], 0)
      }

      def dropAndSplit(comparatorGen: Slice => (Int => Ordering), slices: StreamT[M, Slice], spanStart: Int): StreamT[M, Slice] = StreamT.wrapEffect {
        slices.uncons map {
          case Some((head, tail)) =>
            val headComparator = comparatorGen(head)
            val spanEnd = findEnd(headComparator, spanStart, head.size - 1)
            if (spanEnd < head.size) {
              stepPartition(head, spanEnd, tail)
            } else {
              dropAndSplit(comparatorGen, tail, 0)
            }
            
          case None =>
            StreamT.empty[M, Slice]
        }
      }

      def stepPartition(head: Slice, spanStart: Int, tail: StreamT[M, Slice]): StreamT[M, Slice] = {
        val comparatorGen = (s: Slice) => {
          val rowComparator = Slice.rowComparatorFor(head, s) { s0 =>
            s0.columns.keys collect {
              case ColumnRef(path @ CPath(CPathField("0"), _ @ _*), _) => path
            }
          }

          (i: Int) => rowComparator.compare(spanStart, i)
        }
        
        val groupTable = subTable(comparatorGen, head.drop(spanStart) :: tail)
        val groupedM = groupTable.map(_.transform(DerefObjectStatic(Leaf(Source), CPathField("1")))).flatMap(f)
        val groupedStream: StreamT[M, Slice] = StreamT.wrapEffect(groupedM.map(_.slices))

        groupedStream ++ dropAndSplit(comparatorGen, head :: tail, spanStart)
      }

      val keyTrans = OuterObjectConcat(
        WrapObject(partitionBy, "0"),
        WrapObject(Leaf(Source), "1")
      )

      this.transform(keyTrans).compact(TransSpec1.Id).slices.uncons map {
        case Some((head, tail)) =>
          Table(stepPartition(head, 0, tail), UnknownSize)
        case None =>
          Table.empty
      }
    }

    def normalize: Table = Table(slices.filter(!_.isEmpty), size)

    def schemas: M[Set[JType]] = {

      // Returns true iff masks contains an array equivalent to mask.
      def contains(masks: List[Array[Int]], mask: Array[Int]): Boolean = {

        @tailrec
        def equal(x: Array[Int], y: Array[Int], i: Int): Boolean = if (i >= x.length) {
          true
        } else if (x(i) != y(i)) {
          false
        } else {
          equal(x, y, i + 1)
        }

        @tailrec
        def loop(xs: List[Array[Int]], y: Array[Int]): Boolean = xs match {
          case x :: xs if x.length == y.length  && equal(x, y, 0) => true
          case _ :: xs => loop(xs, y)
          case Nil => false
        }

        loop(masks, mask)
      }

      def isZero(x: Array[Int]): Boolean = {
        @tailrec def loop(i: Int): Boolean = if (i < 0) {
          true
        } else if (x(i) != 0) {
          false
        } else {
          loop(i - 1)
        }

        loop(x.length - 1)
      }

      // Constructs a schema from a set of defined ColumnRefs. Metadata is
      // ignored and there can be no unions. The set of ColumnRefs must all be
      // defined and hence must create a valid JSON object.
      def mkSchema(cols: List[ColumnRef]): Option[JType] = {
        def leafType(ctype: CType): JType = ctype match {
          case CBoolean => JBooleanT
          case CLong | CDouble | CNum => JNumberT
          case CString => JTextT
          case CDate => JTextT
          case CArrayType(elemType) => leafType(elemType)
          case CEmptyObject => JObjectFixedT(Map.empty)
          case CEmptyArray => JArrayFixedT(Map.empty)
          case CNull => JNullT
        }

        def fresh(paths: List[CPathNode], leaf: JType): Option[JType] = paths match {
          case CPathField(field) :: paths => fresh(paths, leaf) map { tpe => JObjectFixedT(Map(field -> tpe)) }
          case CPathIndex(i) :: paths => fresh(paths, leaf) map { tpe => JArrayFixedT(Map(i -> tpe)) }
          case CPathArray :: paths => fresh(paths, leaf) map (JArrayHomogeneousT(_))
          case CPathMeta(field) :: _ => None
          case Nil => Some(leaf)
        }

        def merge(schema: Option[JType], paths: List[CPathNode], leaf: JType): Option[JType] = (schema, paths) match {
          case (Some(JObjectFixedT(fields)), CPathField(field) :: paths) =>
            merge(fields get field, paths, leaf) map { tpe =>
              JObjectFixedT(fields + (field -> tpe))
            } orElse schema
          case (Some(JArrayFixedT(indices)), CPathIndex(idx) :: paths) =>
            merge(indices get idx, paths, leaf) map { tpe =>
              JArrayFixedT(indices + (idx -> tpe))
            } orElse schema
          case (None, paths) =>
            fresh(paths, leaf)
          case (jtype, paths) =>
            sys.error("Invalid schema.") // This shouldn't happen for any real data.
        }

        cols.foldLeft(None: Option[JType]) { case (schema, ColumnRef(cpath, ctype)) =>
          merge(schema, cpath.nodes, leafType(ctype))
        }
      }

      // Collects all possible schemas from some slices.
      def collectSchemas(schemas: Set[JType], slices: StreamT[M, Slice]): M[Set[JType]] = slices.uncons flatMap {
        case Some((slice, slices)) =>
          import java.util.Arrays.copyOf

          val (refs0, cols0) = slice.columns.unzip
          val cols: Array[Column] = cols0.toArray
          val refs: List[(ColumnRef, Int)] = refs0.zipWithIndex.toList

          var masks: List[Array[Int]] = Nil
          val mask: Array[Int] = RawBitSet.create(cols.length)
          Loop.range(0, slice.size) { row =>
            RawBitSet.clear(mask)

            var i = 0
            while (i < cols.length) {
              if (cols(i) isDefinedAt row)
                RawBitSet.set(mask, i)
              i += 1
            }

            if (!contains(masks, mask) && !isZero(mask)) {
              masks = copyOf(mask, mask.length) :: masks
            }
          }

          collectSchemas(schemas ++ (masks flatMap { schemaMask =>
            mkSchema(refs collect { case (ref, i) if RawBitSet.get(schemaMask, i) => ref })
          }), slices)

        case None =>
          M.point(schemas)
      }

      collectSchemas(Set.empty, slices)
    }

    def renderJson(delimiter: Char = '\n'): StreamT[M, CharBuffer] = {
      def delimiterBuffer = {
        val back = CharBuffer.allocate(1)
        back.put(delimiter)
        back.flip()
        back
      }
      
      def delimitStream = delimiterBuffer :: StreamT.empty[M, CharBuffer]
      
      def foldFlatMap(slices: StreamT[M, Slice], rendered: Boolean): StreamT[M, CharBuffer] = {
        StreamT[M, CharBuffer](slices.step map {
          case StreamT.Yield(slice, tail) => {
            val (stream, rendered2) = slice.renderJson[M](delimiter)
            
            val stream2 = if (rendered && rendered2)
              delimitStream ++ stream
            else
              stream
            
            StreamT.Skip(stream2 ++ foldFlatMap(tail(), rendered || rendered2))
          }
          
          case StreamT.Skip(tail) => StreamT.Skip(foldFlatMap(tail(), rendered))
          
          case StreamT.Done => StreamT.Done
        })
      }
      
      foldFlatMap(slices, false)
    }

    def slicePrinter(prelude: String)(f: Slice => String): Table = {
      Table(StreamT(StreamT.Skip({println(prelude); slices map { s => println(f(s)); s }}).point[M]), size)
    }

    def logged(logger: Logger, logPrefix: String = "", prelude: String = "", appendix: String = "")(f: Slice => String): Table = {
      val preludeEffect = StreamT(StreamT.Skip({logger.debug(logPrefix + " " + prelude); StreamT.empty[M, Slice]}).point[M])
      val appendixEffect = StreamT(StreamT.Skip({logger.debug(logPrefix + " " + appendix); StreamT.empty[M, Slice]}).point[M])
      val sliceEffect = if (logger.isTraceEnabled) slices map { s => logger.trace(logPrefix + " " + f(s)); s } else slices
      Table(preludeEffect ++ sliceEffect ++ appendixEffect, size)
    }

    def printer(prelude: String = "", flag: String = ""): Table = slicePrinter(prelude)(s => s.toJsonString(flag))

    def toStrings: M[Iterable[String]] = {
      toEvents { (slice, row) => slice.toString(row) }
    }
    
    def toJson: M[Iterable[JValue]] = {
      toEvents { (slice, row) => slice.toJson(row) }
    }

    private def toEvents[A](f: (Slice, RowId) => Option[A]): M[Iterable[A]] = {
      for (stream <- self.compact(Leaf(Source)).slices.toStream) yield {
        for (slice <- stream; i <- 0 until slice.size; a <- f(slice, i)) yield a
      }
    }

    def metrics = TableMetrics(readStarts.get, blockReads.get)
  }
}
// vim: set ts=4 sw=4 et:
