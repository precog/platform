package com.precog.quirrel

import com.codecommit.gll.LineStream

import com.precog.util._

import scala.collection.mutable

/**
 * Not ''really'' the full compiler.  Technically, this trait just gives you the
 * function you need to turn an input stream into an attributed tree, provided
 * you mix in the other requisite traits.
 */
trait Compiler extends Phases with parser.Parser with typer.TreeShaker with parser.QuirrelCache {
  private val cache = new Cache()

  //def compile(str: LineStream): Set[Expr] = cache.getOrElseUpdate(str)(parse) map shakeTree
  def compile(str: LineStream): Set[Expr] = cache.getOrElseUpdate(str)(parse(_) map shakeTree)
  def compile(str: String): Set[Expr] = {
    val ls = LineStream(str)
    assert(ls.toString == str)
    compile(ls)
  }
}
