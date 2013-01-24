package com.precog
package daze

import com.precog.bytecode._
import com.precog.common.json._

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

trait ArrayLib[M[+_]] extends GenOpcode[M] with Evaluator[M] {
  override def _libMorphism1 = super._libMorphism1 ++ Set(Flatten)
  
  object Flatten extends Morphism1(Vector(), "flatten") {
    import trans._
    import TransSpecModule._
    import scalaz.syntax.monad._
    
    val tpe = UnaryOperationType(JArrayUnfixedT, JType.JUniverseT)
    
    def apply(table: Table, ctx: EvaluationContext) = M point {
      var totalMaxLength = 0      // TODO can probably get better results from avg length
      
      val derefed = table transform trans.DerefObjectStatic(Leaf(Source), paths.Value)
      
      val slices2 = derefed.slices map { slice =>
        val maxLength = (slice.columns.keys collect {
          case ColumnRef(CPath(CPathIndex(i), _ @ _*), _) => i
        } max) + 1
        
        totalMaxLength = totalMaxLength max maxLength
        
        val columns2 = slice.columns.foldLeft(Map[ColumnRef, Column]()) {
          case (acc, (ColumnRef(CPath(CPathIndex(idx), ptail @ _*), tpe), col)) => {
            // remap around the mod ring w.r.t. max length
            // s.t. f(i) = f'(i * max + arrayI)
            val remap = cf.util.Remap { idx2 =>
              (idx2 - idx) / maxLength
            }
            
            val col2 = remap(col).get   // known to be safe
            
            // remove indices which don't make sense
            val definedComplement = new NullColumn {
              def isDefinedAt(idx2: Int) = (idx2 - idx) % maxLength != 0
            }
            
            val col3 = cf.util.FilterComplement(definedComplement)(col2).get
            
            val finalRef = ColumnRef(CPath(ptail: _*), tpe)
            val finalCol = acc get finalRef flatMap { accCol =>
              cf.util.UnionRight(accCol, col3)
            } getOrElse col3
            
            acc.updated(finalRef, finalCol)
          }
          
          case (acc, _) => acc
        }
        
        Slice(columns2, slice.size * maxLength)
      }
      
      val size2 = table.size * EstimateSize(0, totalMaxLength)
      val table2 = Table(slices2, size2) paged yggConfig.maxSliceSize compact TransSpec1.Id
      
      val idSpec =
        InnerObjectConcat(
          WrapObject(
            trans.WrapArray(Scan(Leaf(Source), freshIdScanner)),
            paths.Key.name),
        trans.WrapObject(Leaf(Source), paths.Value.name))
      
      table2 transform idSpec
    }
  }
}
