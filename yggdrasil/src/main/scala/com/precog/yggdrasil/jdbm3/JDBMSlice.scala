package com.precog.yggdrasil
package jdbm3

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import java.nio.ByteBuffer
import java.util.SortedMap

import com.precog.util.Bijection._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization.bijections._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scala.collection.JavaConverters._

import JDBMProjection._

/**
 * A slice built from a JDBMProjection
 *
 * @param source The source SortedMap containing the key/value pairs
 * @param descriptor The descriptor for the projection that this slice represents
 * @param size How many entries to retrieve in this slice
 */
class JDBMSlice private[jdbm3](source: IndexTree, descriptor: ProjectionDescriptor, columnConstraint: Set[ColumnDescriptor], requestedSize: Int) extends Slice with Logging {
  trait BaseColumn {
    def isDefinedAt(row: Int) = row < size
  }
  
  // This is where we store the full 2d array containing all values. Ugly cast to allow us to use IndexedSeq internally
  private[this] val backing: Array[java.util.Map.Entry[Identities,IndexedSeq[CValue]]] = source.entrySet().iterator().asScala.take(size).toArray.asInstanceOf[Array[java.util.Map.Entry[Identities,IndexedSeq[CValue]]]]

  def size = backing.length

  case class IdentColumn(index: Int) extends LongColumn with BaseColumn {
    def apply(row: Int): Long = backing(row).getKey.apply(index)
  }

  def firstKey: Identities = backing(0).getKey
  def lastKey: Identities  = backing(size - 1).getKey

  protected def keyColumns: Map[ColumnRef, Column] = (0 until descriptor.identities).map {
    idx: Int => ColumnRef(JPath(JPathField("key") :: JPathIndex(idx) :: Nil), CLong) -> IdentColumn(idx)
  }.toMap

  protected def desiredColumns: Seq[(ColumnDescriptor,Int)] = {
    if (columnConstraint.isEmpty) {
      descriptor.columns.zipWithIndex
    } else {
      descriptor.columns.zipWithIndex.filter { case (desc, _) => columnConstraint.contains(desc) }
    }
  }

  def valColumns: Seq[(ColumnRef, Column)] = desiredColumns.map {
    case (ColumnDescriptor(_, selector, ctpe, _),index) => ColumnRef(selector, ctpe) -> (ctpe match {
      //// Fixed width types within the var width row
      case CBoolean => new BoolColumn with BaseColumn {
        def apply(row: Int): Boolean = backing(row).getValue().apply(index).asInstanceOf[java.lang.Boolean]
      }

      case  CLong  => new LongColumn with BaseColumn {
        def apply(row: Int): Long = backing(row).getValue().apply(index).asInstanceOf[java.lang.Long]
      }

      case CDouble => new DoubleColumn with BaseColumn {
        def apply(row: Int): Double = backing(row).getValue().apply(index).asInstanceOf[java.lang.Double]
      }

      case CDate => new DateColumn with BaseColumn {
        def apply(row: Int): DateTime = new DateTime(backing(row).getValue().apply(index).asInstanceOf[java.lang.Long])
      }

      case CNull => LNullColumn
      
      case CEmptyObject => LEmptyObjectColumn
      
      case CEmptyArray => LEmptyArrayColumn

      //// Variable width types
      case CString => new StrColumn with BaseColumn {
        def apply(row: Int): String = backing(row).getValue().apply(index).asInstanceOf[String]
      }

      case CNum => new NumColumn with BaseColumn {
        def apply(row: Int): BigDecimal = BigDecimal(backing(row).getValue().apply(index).asInstanceOf[java.math.BigDecimal])
      }

      case invalid => sys.error("Invalid fixed with CType: " + invalid)
    })
  }

  lazy val columns: Map[ColumnRef, Column] = keyColumns ++ valColumns
  
  object LNullColumn extends table.NullColumn with BaseColumn
  object LEmptyObjectColumn extends table.EmptyObjectColumn with BaseColumn
  object LEmptyArrayColumn extends table.EmptyArrayColumn with BaseColumn
}
