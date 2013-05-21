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
package com.precog.daze

import com.precog.common._
import com.precog.bytecode._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata.StorageMetadata
import com.precog.yggdrasil.table._

import java.util.regex._
import scalaz.StreamT
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

trait FSLibModule[M[+_]] extends ColumnarTableLibModule[M] with StorageMetadataSource[M] {
  import trans._

  trait FSLib extends ColumnarTableLib {
    import constants._
  
    val FSNamespace = Vector("std", "fs")
    override def _libMorphism1 = super._libMorphism1 ++ Set(expandGlob)
  
    object expandGlob extends Morphism1(FSNamespace, "expandGlob") {
      val tpe = UnaryOperationType(JTextT, JTextT)
  
      val pattern = Pattern.compile("""/+((?:[a-zA-Z0-9\-\._~:?#@!$&'+=]+)|\*)""")
  
      def expand_*(pathString: String, pathRoot: Path, metadata: StorageMetadata[M]): M[Stream[Path]] = {
        def traverse(m: Matcher, prefixes: Stream[Path]): M[Stream[Path]] = {
          if (m.find) {
            m.group(1).trim match {
              case "*" => 
                prefixes.map { prefix =>
                  metadata.findDirectChildren(prefix) map { child =>
                    child map { prefix / _  }
                  }
                }.sequence flatMap { paths =>
                  traverse(m, paths.flatten)
                }
  
              case token =>
                traverse(m, prefixes.map(_ / Path(token)))
            }
          } else {
            M.point(prefixes)
          }
        }
  
        traverse(pattern.matcher(pathString), Stream(pathRoot))
      }
  
      def apply(input: Table, ctx: MorphContext): M[Table] = M.point {
        val storageMetadata = userMetadataView(ctx.evalContext.account.apiKey)
        val result = Table(
          input.transform(SourceValue.Single).slices flatMap { slice =>
            slice.columns.get(ColumnRef.identity(CString)) collect { 
              case col: StrColumn => 
                val expanded: Stream[M[Stream[Path]]] = Stream.tabulate(slice.size) { i =>
                  expand_*(col(i), ctx.evalContext.basePath, storageMetadata)
                }
  
                StreamT wrapEffect {
                  expanded.sequence map { pathSets => 
                    val unprefixed: Stream[String] = for {
                      paths <- pathSets
                      path <- paths
                      suffix <- (path - ctx.evalContext.basePath)
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
