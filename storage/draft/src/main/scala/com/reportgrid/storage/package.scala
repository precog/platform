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