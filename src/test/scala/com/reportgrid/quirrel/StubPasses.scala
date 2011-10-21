package com.reportgrid.quirrel

trait StubPasses extends Passes with RawErrors {
  def bindNames(tree: Expr) = Set()
  def checkProvenance(tree: Expr) = Set()
}
