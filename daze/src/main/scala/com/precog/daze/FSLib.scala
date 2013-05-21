package com.precog.daze

import com.precog.common._
import com.precog.common.security._
import com.precog.bytecode._
import com.precog.yggdrasil._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.vfs._

import java.util.regex._
import scalaz.StreamT
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

trait FSLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  def vfs: SecureVFS[M]

  import trans._

  trait FSLib extends ColumnarTableLib {
    import constants._
  
    val FSNamespace = Vector("std", "fs")
    override def _libMorphism1 = super._libMorphism1 ++ Set(expandGlob)
  
    object expandGlob extends Morphism1(FSNamespace, "expandGlob") {
      val tpe = UnaryOperationType(JTextT, JTextT)
  
      val pattern = Pattern.compile("""/+((?:[a-zA-Z0-9\-\._~:?#@!$&'+=]+)|\*)""")
  
      def expand_*(apiKey: APIKey, pathString: String, pathRoot: Path): M[Stream[Path]] = {
        def walk(m: Matcher, prefixes: Stream[Path]): M[Stream[Path]] = {
          if (m.find) {
            m.group(1).trim match {
              case "*" => 
                prefixes traverse { prefix =>
                  vfs.findDirectChildren(apiKey, prefix) map { child =>
                    child map { prefix / _  }
                  }
                } flatMap { paths =>
                  walk(m, paths.flatten)
                }
  
              case token =>
                walk(m, prefixes.map(_ / Path(token)))
            }
          } else {
            M.point(prefixes)
          }
        }
  
        walk(pattern.matcher(pathString), Stream(pathRoot))
      }
  
      def apply(input: Table, ctx: EvaluationContext): M[Table] = M.point {
        val result = Table(
          input.transform(SourceValue.Single).slices flatMap { slice =>
            slice.columns.get(ColumnRef.identity(CString)) collect { 
              case col: StrColumn => 
                val expanded: Stream[M[Stream[Path]]] = Stream.tabulate(slice.size) { i =>
                  expand_*(ctx.apiKey, col(i), ctx.basePath)
                }
  
                StreamT wrapEffect {
                  expanded.sequence map { pathSets => 
                    val unprefixed: Stream[String] = for {
                      paths <- pathSets
                      path <- paths
                      suffix <- (path - ctx.basePath)
                    } yield suffix.toString
  
                    Table.constString(unprefixed.toSet).slices 
                  }
                }
            } getOrElse {
              StreamT.empty[M, Slice]
            }
          },
          UnknownSize
        )
        
        result.transform(WrapObject(Leaf(Source), TransSpecModule.paths.Value.name))
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
