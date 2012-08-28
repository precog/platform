package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.VectorCase

import akka.actor.ActorSystem

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scala.annotation.tailrec
import scala.collection.BitSet

import scalaz._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

trait TestColumnarTableModule[M[+_]] extends ColumnarTableModule[M] {
  implicit def coM: Copointed[M]

  def fromJson(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table = {
    val sliceSize = maxSliceSize.getOrElse(10)

    def makeSlice(sampleData: Stream[JValue]): (Slice, Stream[JValue]) = {
      val (prefix, suffix) = sampleData.splitAt(sliceSize)
  
      @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int): (Map[ColumnRef, (BitSet, Object)], Int) = {
        from match {
          case jv #:: xs =>
            val withIdsAndValues = jv.flattenWithPath.foldLeft(into) {
              case (acc, (jpath, JNothing)) => acc
              case (acc, (jpath, v)) =>
                val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
                val ref = ColumnRef(jpath, ctype)
  
                val pair: (BitSet, Array[_]) = v match {
                  case JBool(b) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Boolean](sliceSize))).asInstanceOf[(BitSet, Array[Boolean])]
                    col(sliceIndex) = b
                    (defined + sliceIndex, col)
                    
                  case JNum(d) => {
                    val isLong = ctype == CLong
                    val isDouble = ctype == CDouble
                    
                    val (defined, col) = if (isLong) {
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Long](sliceSize))).asInstanceOf[(BitSet, Array[Long])]
                      col(sliceIndex) = d.toLong
                      (defined, col)
                    } else if (isDouble) {
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Double](sliceSize))).asInstanceOf[(BitSet, Array[Double])]
                      col(sliceIndex) = d.toDouble
                      (defined, col)
                    } else {
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[BigDecimal](sliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                      col(sliceIndex) = d
                      (defined, col)
                    }
                    
                    (defined + sliceIndex, col)
                  }
  
                  case JString(s) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[String](sliceSize))).asInstanceOf[(BitSet, Array[String])]
                    col(sliceIndex) = s
                    (defined + sliceIndex, col)
                  
                  case JArray(Nil)  => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                    (defined + sliceIndex, col)
  
                  case JObject(Nil) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                    (defined + sliceIndex, col)
  
                  case JNull        => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                    (defined + sliceIndex, col)
                }
  
                acc + (ref -> pair)
            }
  
            buildColArrays(xs, withIdsAndValues, sliceIndex + 1)
  
          case _ => (into, sliceIndex)
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
    
    table(
      StreamT.unfoldM(values) { events =>
        M.point {
          (!events.isEmpty) option {
            makeSlice(events.toStream)
          }
        }
      }
    )
  }

  def debugPrint(dataset: Table): Unit = {
    println("\n\n")
    dataset.slices.foreach { slice => {
      M.point(for (i <- 0 until slice.size) println(slice.toString(i)))
    }}
  }
}

trait StubColumnarTableModule[M[+_]] extends TestColumnarTableModule[M] {
  type Table = StubTable

  def table(slices: StreamT[M, Slice]): StubTable = new StubTable(slices)

  type MemoContext = DummyMemoizationContext
  def newMemoContext = new DummyMemoizationContext
  
  class StubTable(slices: StreamT[M, Slice]) extends ColumnarTable(slices) { self: Table => 
    private var initialIndices = collection.mutable.Map[Path, Int]()
    private var currentIndex = 0

    import trans._
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): M[Table] = {
      // We use the sort transspec1 to compute a new table with a combination of the 
      // original data and the new sort columns, referenced under the sortkey namespace
      val tableWithSortKey = transform(ObjectConcat(Leaf(Source), WrapObject(sortKey, TableModule.paths.SortKey.name)))

      implicit val jValueOrdering = blueeyes.json.xschema.DefaultOrderings.JValueOrdering

      tableWithSortKey.toJson.map {
        jvals => fromJson(jvals.toList.sorted.toStream)
      }
    }
    
    override def load(uid: UserId, jtpe: JType) = {
      self.toJson map { events =>
        fromJson {
          events.toStream flatMap {
            case JString(pathStr) => 
              val path = Path(pathStr)
        
              val index = initialIndices get path getOrElse {
                initialIndices += (path -> currentIndex)
                currentIndex
              }
              
              val target = path.path.replaceAll("/$", ".json")
              val src = io.Source fromInputStream getClass.getResourceAsStream(target)
              val parsed = src.getLines map JsonParser.parse toStream
              
              currentIndex += parsed.length
              
              parsed zip (Stream from index) map {
                case (value, id) => JObject(JField("key", JArray(JNum(id) :: Nil)) :: JField("value", value) :: Nil)
              }

            case x => sys.error("Attempted to load JSON as a table from something that wasn't a string: " + x)
          }
        }
      }
    }

    override def toString = toStrings.copoint.mkString("\n")
  }
}

// vim: set ts=4 sw=4 et:
