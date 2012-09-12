package com.precog.quirrel

import com.codecommit.gll.LineStream

/**
 * Not ''really'' the full compiler.  Technically, this trait just gives you the
 * function you need to turn an input stream into an attributed tree, provided
 * you mix in the other requisite traits.
 */
trait Compiler extends Phases with parser.Parser with typer.TreeShaker {
  def compile(str: LineStream): Expr = shakeTree(parse(str))
  def compile(str: String): Expr = compile(LineStream(str))
}
