package com.precog.yggdrasil.table

import org.joda.time.DateTime
import scala.collection.BitSet

class BitsetColumn(definedAt: BitSet) { this: Column =>
  def isDefinedAt(row: Int): Boolean = definedAt(row)

  override def toString = {
    val limit = definedAt.reduce(_ max _)
    val repr = (row: Int) => if (definedAt(row)) 'x' else '_'
    getClass.getName + "(" + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

object BitsetColumn {
  def bitset(definedAt: Seq[Boolean]) = {
    BitSet(definedAt.zipWithIndex collect { case (v, i) if v => i }: _*)
  }
}

class Map1Column(c: Column) { this: Column =>
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
}

class Map2Column(c1: Column, c2: Column) { this: Column =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
}

class UnionColumn[T <: Column](c1: T, c2: T) { this: T =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row) || c2.isDefinedAt(row)
}

class ConcatColumn[T <: Column](at: Int, c1: T, c2: T) { this: T =>
  def isDefinedAt(row: Int) = row >= 0 && ((row < at && c1.isDefinedAt(row)) || (row >= at && c2.isDefinedAt(row - at)))
}

class ShiftColumn[T <: Column](by: Int, c1: T) { this: T =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row + by)
}

class RemapColumn[T <: Column](delegate: T, f: PartialFunction[Int, Int]) { this: T =>
  def isDefinedAt(row: Int) = f.isDefinedAt(row) && delegate.isDefinedAt(f(row))
}

class InfiniteColumn { this: Column =>
  def isDefinedAt(row: Int) = true
}


/* help for ctags
type ColumnSupport */
