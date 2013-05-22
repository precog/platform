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

import blueeyes.json._
import com.precog.bytecode.JType
import com.precog.common._
import com.precog.util._

sealed trait CFId
case class LeafCFId(identity: String) extends CFId
case class ComposedCFId(l: CFId, r: CFId) extends CFId
case class PartialLeftCFId(cv: CValue, r: CFId) extends CFId
case class PartialRightCFId(l: CFId, cv: CValue) extends CFId

object CFId {
  def apply(identity: String) = LeafCFId(identity)
}

trait CF {
  def identity: CFId
  override final def equals(other: Any): Boolean = other match {
    case cf: CF => identity == cf.identity 
    case _ => false
  }
  
  override final def hashCode: Int = identity.hashCode
  
  override def toString() = identity.toString  
}

trait CF1 extends CF { self =>
  def apply(c: Column): Option[Column]
  def apply(cv: CValue): Option[Column] = apply(Column.const(cv))

  // Do not use PartialFunction.compose or PartialFunction.andThen for composition,
  // because they will fail out with MatchError.
  def compose(f1: CF1): CF1 = new CF1 {
    def apply(c: Column) = f1(c).flatMap(self.apply)
    val identity = ComposedCFId(f1.identity, self.identity)
  }

  def andThen(f1: CF1): CF1 = new CF1 {
    def apply(c: Column) = self.apply(c).flatMap(f1.apply)
    val identity = ComposedCFId(self.identity, f1.identity)
  }
}

object CF1 {
  def apply(name: String)(f: Column => Option[Column]): CF1 = apply(CFId(name))(f)
  def apply(id: CFId)(f: Column => Option[Column]): CF1 = new CF1 {
    def apply(c: Column) = f(c)
    val identity = id
  }
}

object CF1P {
  def apply(name: String)(f: PartialFunction[Column, Column]): CF1 = apply(CFId(name))(f)
  def apply(id: CFId)(f: PartialFunction[Column, Column]): CF1 = new CF1 {
    def apply(c: Column) = f.lift(c)
    val identity = id
  }
}

object CF1Array {
  def apply[A](name: String)(pf: PartialFunction[(Column, Range), (CType, Array[Array[A]], BitSet)]): CScanner = new ArrayScanner {
    def apply(columns0: Map[ColumnRef, Column], range: Range) = {
      columns0 collect {
        case (ColumnRef(CPath.Identity, _), col) if pf isDefinedAt (col, range) => {
          val (tpe, cols, defined) = pf((col, range))
          tpe -> (cols.asInstanceOf[Array[Array[_]]], defined)
        }
      }
    }
  }
}

trait CF2 extends CF { self =>
  def apply(c1: Column, c2: Column): Option[Column] 
  
  @inline
  def partialLeft(cv: CValue): CF1 = {
    new CF1 {
      def apply(c2: Column) = self.apply(Column.const(cv), c2)
      val identity = PartialLeftCFId(cv, self.identity)
    }
  }
  
  @inline
  def partialRight(cv: CValue): CF1 = {
    new CF1 {
      def apply(c1: Column) = self.apply(c1, Column.const(cv))
      val identity = PartialRightCFId(self.identity, cv)
    }
  }
}

object CF2 {
  def apply(id: String)(f: (Column, Column) => Option[Column]): CF2 = apply(CFId(id))(f)
  def apply(id: CFId)(f: (Column, Column) => Option[Column]): CF2 = new CF2 {
    def apply(c1: Column, c2: Column) = f(c1, c2)
    val identity = id
  }
}

object CF2P {
  def apply(id: String)(f: PartialFunction[(Column, Column), Column]): CF2 = apply(CFId(id))(f)
  def apply(id: CFId)(f: PartialFunction[(Column, Column), Column]): CF2 = new CF2 {
    def apply(c1: Column, c2: Column) = f.lift((c1, c2))
    val identity = id
  }
}

object CF2Array {
  def apply[A](name: String)(pf: PartialFunction[(Column, Column, Range), (CType, Array[Array[A]], BitSet)]): CScanner = new ArrayScanner {
    def apply(columns0: Map[ColumnRef, Column], range: Range) = {
      for {
        (ColumnRef(CPath(CPathIndex(0)), _), col1) <- columns0
        (ColumnRef(CPath(CPathIndex(1)), _), col2) <- columns0
        if pf isDefinedAt (col1, col2, range)
      } yield {
        val (tpe, cols, defined) = pf((col1, col2, range))
        tpe -> (cols.asInstanceOf[Array[Array[_]]], defined)
      }
    }
  }
}

trait CScanner {
  type A
  def init: A
  def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column])
}

trait CSchema {
  def columnRefs: Set[ColumnRef]
  def columns(jtype: JType): Set[Column]
}

trait CReducer[A] {
  def reduce(schema: CSchema, range: Range): A
}

trait ArrayScanner extends CScanner {
  import org.joda.time.{DateTime, Period}
  
  type A = Unit
  
  def init = ()
  
  def scan(a: Unit, columns0: Map[ColumnRef, Column], range: Range): (Unit, Map[ColumnRef, Column]) = {
    val results = this(columns0, range)
    
    val columns = results flatMap {
      case (tpe @ CString, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[String]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new StrColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CBoolean, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Boolean]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new BoolColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CLong, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Long]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new LongColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CDouble, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Double]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new DoubleColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CNum, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[BigDecimal]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new NumColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CNull, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Unit]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new NullColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CEmptyObject, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Unit]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new EmptyObjectColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CEmptyArray, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Unit]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new EmptyArrayColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CDate, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[DateTime]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new DateColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe @ CPeriod, (cols0, defined)) => {
        val max = maxIds(cols0, defined)
        val cols = cols0.asInstanceOf[Array[Array[Period]]]
        
        val columns: Map[ColumnRef, Column] = (0 until max).map({ i =>
          ColumnRef(CPath(CPathIndex(i)), tpe) -> new PeriodColumn {
            def isDefinedAt(row: Int) = defined.get(row) && i < cols(row).length
            def apply(row: Int) = cols(row)(i)
          }
        })(collection.breakOut)
        
        columns
      }
      
      case (tpe, _) => sys.error("Unsupported CFArray type: " + tpe)
    }
    
    ((), columns)
  }
  
  def apply(columns0: Map[ColumnRef, Column], range: Range): Map[CType, (Array[Array[_]], BitSet)]
    
  private[this] def maxIds(arr: Array[Array[_]], mask: BitSet): Int = {
    var back = -1
    0 until arr.length foreach { i =>
      if (mask get i) {
        back = back max arr(i).length
      }
    }
    back
  }
}


/* ctags
type FN */
