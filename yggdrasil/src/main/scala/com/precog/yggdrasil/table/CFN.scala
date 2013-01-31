package com.precog.yggdrasil
package table

import blueeyes.json._
import com.precog.bytecode.JType

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


/* ctags
type FN */
