package com.reportgrid.quirrel

trait StubPhases extends Phases with RawErrors {
  def bindNames(tree: Expr) = Set()
  def checkProvenance(tree: Expr) = Set()
}
