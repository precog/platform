package com.reportgrid.quirrel

trait StubPasses extends Passes with StringErrors {
  def bindNames(tree: Expr) = Set()
}
