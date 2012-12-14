package com.precog.daze

import com.precog.bytecode._
import com.precog.common.Path
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata.StorageMetadata
import com.precog.yggdrasil.table._

import java.util.regex._
import scalaz.StreamT
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

trait FSLib[M[+_]] extends GenOpcode[M] {
  val FSNamespace = Vector("std", "fs")
  override def _libMorphism1 = super._libMorphism1 ++ Set(expandGlob)

  def storage: StorageMetadataSource[M]

  object expandGlob extends Morphism1(FSNamespace, "expandGlob") {
    val tpe = UnaryOperationType(JTextT, JTextT)

    val pattern = Pattern.compile("""/+((?:[a-zA-Z0-9\-\._~:?#@!$&'+=]+)|\*)""")

    def expand_*(pathString: String, pathRoot: Path, metadata: StorageMetadata[M]): M[Stream[Path]] = {
      println("expanding any globs in path: " + pathString)
      def traverse(m: Matcher, prefixes: Stream[Path]): M[Stream[Path]] = {
        if (m.find) {
          m.group(1).trim match {
            case "*" => 
              prefixes.map(prefix => metadata.findChildren(prefix) map { _ map { prefix / _  } }).sequence flatMap { paths =>
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

    def apply(input: Table, ctx: EvaluationContext): M[Table] = M.point {
      val storageMetadata = storage.userMetadataView(ctx.apiKey)
      Table(
        input.slices flatMap { slice =>
          slice.columns.get(ColumnRef.identity(CString)) collect { 
            case col: StrColumn => 
              val expanded: Stream[M[Stream[Path]]] = Stream.tabulate(slice.size) { i =>
                expand_*(col(i), ctx.basePath, storageMetadata)
              }

              StreamT wrapEffect {
                expanded.sequence map { paths => 
                  println("Constructing table of expanded paths from " + paths.toSet)
                  Table.constString(paths.flatten.map(p => CString(p.toString)).toSet).slices 
                }
              }
          } getOrElse {
            StreamT.empty[M, Slice]
          }
        },
        UnknownSize
      )
    }
  }
}

// vim: set ts=4 sw=4 et:
