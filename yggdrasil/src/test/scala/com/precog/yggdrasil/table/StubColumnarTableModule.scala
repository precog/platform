package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.VectorCase

import akka.actor.ActorSystem
import akka.dispatch.{Future, ExecutionContext}

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scala.annotation.tailrec
import scala.collection.BitSet

import scalaz._

trait TestColumnarTableModule extends ColumnarTableModule {
  def fromJson(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table = {
    val sliceSize = maxSliceSize.getOrElse(10)

    def makeSlice(sampleData: Iterable[JValue]): (Slice, Iterable[JValue]) = {
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
  
                  case JInt(ji) => CType.sizedIntCValue(ji) match {
                    case CLong(v) =>
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Long](sliceSize))).asInstanceOf[(BitSet, Array[Long])]
                      col(sliceIndex) = v
                      (defined + sliceIndex, col)
  
                    case CNum(v) =>
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[BigDecimal](sliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                      col(sliceIndex) = v
                      (defined + sliceIndex, col)

                    case invalid => sys.error("Invalid size Int type: " + invalid)
                  }
  
                  case JDouble(d) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Double](sliceSize))).asInstanceOf[(BitSet, Array[Double])]
                    col(sliceIndex) = d
                    (defined + sliceIndex, col)
  
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
    
    val (s, xs) = makeSlice(values)
    
    table(new Iterable[Slice] {
      def iterator = new Iterator[Slice] {
        private var _next = s
        private var _rest = xs

        def hasNext = _next != null
        def next() = {
          val tmp = _next
          _next = if (_rest.isEmpty) null else {
            val (s, xs) = makeSlice(_rest)
            _rest = xs
            s
          }
          tmp
        }
      }
    })
  }
}

// vim: set ts=4 sw=4 et:
trait StubColumnarTableModule extends TestColumnarTableModule {
  type Table = StubTable

  val actorSystem = ActorSystem()
  implicit def executionContext: ExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def table(slices: Iterable[Slice]): StubTable = new StubTable(slices)

  class StubTable(slices: Iterable[Slice]) extends ColumnarTable(slices) { self: Table => 
    private var initialIndices = collection.mutable.Map[Path, Int]()
    private var currentIndex = 0
    
    override def load(jtpe: JType) = Future {
      fromJson {
        self.toJson.toStream map (_ \ "value") flatMap {
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
              case (value, id) => JObject(JField("key", JArray(JInt(id) :: Nil)) :: JField("value", value) :: Nil)
            }

          case x => sys.error("Attempted to load JSON as a table from something that wasn't a string: " + x)
        }
      }
    }

    override def toString = toStrings.mkString("\n")
  }
}

