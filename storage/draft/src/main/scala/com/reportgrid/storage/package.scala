package com.reportgrid

package object storage {
  def dataset[I: Reified, A: Reified](path: String) = ExprDataset.Load[I, A](DatasetId(path, None))
  def dataset[I: Reified, A: Reified](path: String, name: String) = ExprDataset.Load[I, A](DatasetId(path, Some(name)))
  
  implicit def BooleanToExprBooleanConstant(v: Boolean) = ExprBool.Constant(v)

  implicit def LongToExprLongConstant(v: Long) = ExprLong.Constant(v)

  // Pimps
  implicit def ToExprBoolPimp(value: Expr[Boolean]): ExprBoolPimp = ExprBoolPimp(value)
  implicit def ToExprLongPimp(value: Expr[Long]) = ExprLongPimp(value)
  implicit def ToExprTuple2Pimp[A: Reified, B: Reified](value: Expr[(A, B)]) = ExprTuple2Pimp(value)
  implicit def ToExprDatasetPimp[I: Reified, A: Reified](value: ExprDataset[I, A]) = ExprDatasetPimp(value)
}