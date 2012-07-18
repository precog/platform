package com.precog.yggdrasil
package table

import com.precog.common.{Path, VectorCase}

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.joda.time.DateTime

import scala.collection.BitSet

class DerefSlice(source: Slice, derefBy: PartialFunction[Int, JPathNode]) extends Slice {
  private val forwardIndex: Map[JPathNode, Map[ColumnRef, Column]] = source.columns.foldLeft(Map.empty[JPathNode, Map[ColumnRef, Column]]) {
    case (acc, (ColumnRef(JPath(root, xs @ _*), ctype), col)) => 
      val resultRef = ColumnRef(JPath(xs: _*), ctype)
      // we know the combination of xs and ctype to be unique within root
      acc + (root -> (acc.getOrElse(root, Map()) + (resultRef -> col)))
  }

  val size = source.size

  val columns = source.columns.keySet.foldLeft(Map.empty[ColumnRef, Column]) {
    case (acc, ColumnRef(JPath(_, xs @ _*), ctype)) => 
      val resultRef = ColumnRef(JPath(xs: _*), ctype)

      lazy val resultCol = ctype match {
        case CBoolean => 
          new BoolColumn {
            private var row0: Int = -1
            private var refCol0: BoolColumn = _
            @inline private def refCol(row: Int): BoolColumn =  
              forwardIndex.get(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[BoolColumn]

            def apply(row: Int) = refCol0(row)

            def isDefinedAt(row: Int) = {
              derefBy.isDefinedAt(row) && { 
                if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                refCol0 != null && refCol0.isDefinedAt(row)
              }
            }
          }

        case CLong =>
          new LongColumn {
            private var row0: Int = -1
            private var refCol0: LongColumn = _
            @inline private def refCol(row: Int): LongColumn = 
              forwardIndex.get(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LongColumn]

            def apply(row: Int) = refCol0(row)

            def isDefinedAt(row: Int) = {
              derefBy.isDefinedAt(row) && { 
                if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                refCol0 != null && refCol0.isDefinedAt(row)
              }
            }
          }

        case CDouble =>
          new DoubleColumn {
            private var row0: Int = -1
            private var refCol0: DoubleColumn = _
            @inline private def refCol(row: Int): DoubleColumn = 
              forwardIndex.get(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DoubleColumn]

            def apply(row: Int) = refCol0(row)

            def isDefinedAt(row: Int) = {
              derefBy.isDefinedAt(row) && { 
                if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                refCol0 != null && refCol0.isDefinedAt(row)
              }
            }
          }

        case CNum =>
          new NumColumn {
            private var row0: Int = -1
            private var refCol0: NumColumn = _
            @inline private def refCol(row: Int): NumColumn = 
              forwardIndex.get(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[NumColumn]

            def apply(row: Int) = refCol0(row)

            def isDefinedAt(row: Int) = {
              derefBy.isDefinedAt(row) && { 
                if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                refCol0 != null && refCol0.isDefinedAt(row)
              }
            }
          }


        case CString =>
          new StrColumn {
            private var row0: Int = -1
            private var refCol0: StrColumn = _
            @inline private def refCol(row: Int): StrColumn = 
              forwardIndex.get(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[StrColumn]

            def apply(row: Int) = refCol0(row)

            def isDefinedAt(row: Int) = {
              derefBy.isDefinedAt(row) && { 
                if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                refCol0 != null && refCol0.isDefinedAt(row)
              }
            }
          }

        case CDate =>
          new DateColumn {
            private var row0: Int = -1
            private var refCol0: DateColumn = _
            @inline private def refCol(row: Int): DateColumn = 
              forwardIndex.get(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DateColumn]

            def apply(row: Int) = refCol0(row)

            def isDefinedAt(row: Int) = {
              derefBy.isDefinedAt(row) && { 
                if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                refCol0 != null && refCol0.isDefinedAt(row)
              }
            }
          }

        case CEmptyObject =>
          new EmptyObjectColumn {
            def isDefinedAt(row: Int) = derefBy.isDefinedAt(row) && 
                                        forwardIndex.get(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
          }

        case CEmptyArray =>
          new EmptyArrayColumn {
            def isDefinedAt(row: Int) = derefBy.isDefinedAt(row) && 
                                        forwardIndex.get(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
          }

        case CNull =>
          new NullColumn {
            def isDefinedAt(row: Int) = derefBy.isDefinedAt(row) && forwardIndex.get(derefBy(row)).exists(cols => cols(resultRef).isDefinedAt(row)) 
          }
      }

      acc + (resultRef -> acc.getOrElse(resultRef, resultCol))
  }
}

/* A strict version
derefBy.columns.headOption collect {
  case c: StrColumn =>
    val transforms: Map[String, Map[ColumnRef, (Column, Column with ArrayColumn[_])]] = 
      slice.columns.foldLeft(Map.empty[String, Map[ColumnRef, (Column, Column with ArrayColumn[_])]]) {
        case (acc, (ColumnRef(JPath(JPathField(root), xs @ _*), ctype), col)) => 
          val resultRef = ColumnRef(JPath(xs: _*), ctype)

          // find the result column if it exists, or else create a new one.
          val resultColumn = acc.get(root).flatMap(_.get(resultRef).map(_._2)).getOrElse(
            ctype match {
              case CBoolean     => ArrayBoolColumn()
              case CLong        => ArrayLongColumn(slice.size)
              case CDouble      => ArrayDoubleColumn(slice.size)
              case CNum         => ArrayNumColumn(slice.size)
              case CString      => ArrayStrColumn(slice.size)
              case CDate        => ArrayDateColumn(slice.size)
              case CEmptyObject => MutableEmptyObjectColumn()
              case CEmptyArray  => MutableEmptyArrayColumn()
              case CNull        => MutableNullColumn()
            }
          )

          val suffixes = acc.getOrElse(root, Map())
          // we know the combination of xs and ctype to be unique within root
          // the result column is shared between common occurrences of resultRef
          // across roots
          acc + (root -> (suffixes + (resultRef -> ((col, resultColumn)))))
      }

    for (i <- 0 until slice.size if c.isDefinedAt(i); cols <- transforms.get(c(i))) {
      cols foreach {
        case (_, (source: BoolColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source(i)

        case (_, (source: LongColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Long]](i) = source(i)

        case (_, (source: DoubleColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Double]](i) = source(i)

        case (_, (source: NumColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[BigDecimal]](i) = source(i)

        case (_, (source: StrColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[String]](i) = source(i)

        case (_, (source: DateColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[DateTime]](i) = source(i)

        case (_, (source: EmptyObjectColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source.isDefinedAt(i)

        case (_, (source: EmptyArrayColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source.isDefinedAt(i)

        case (_, (source: NullColumn, target)) if source.isDefinedAt(i) =>
          target.asInstanceOf[ArrayColumn[Boolean]](i) = source.isDefinedAt(i)

        case _ =>
      }
    } 

    // we can safely throw out any duplicated keys, since the values for duplicated
    // keys are all shared
    transforms.values map { _.mapValues(_._2) } reduceOption { _ ++ _ } getOrElse { Map() }
} getOrElse {
  slice.columns
}
*/
// vim: set ts=4 sw=4 et:
