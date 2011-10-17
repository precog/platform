package com.reportgrid.quirrel

trait StubPasses extends Passes {
  def bindNames(tree: Expr) = Set()
}
