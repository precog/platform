package com.precog
package daze

import com.precog.common._
import bytecode._
import bytecode.Library

import yggdrasil._
import yggdrasil.table._

import com.precog.util._

import java.lang.String

import TransSpecModule._

/* DEPRECATED
 *
 * Currently, we are accepting StrColumn and DateColumn when we need
 * strings. This is because we don't have date literal syntax, so ingest
 * will turn some strings into dates (when they match ISO8601). But
 * in cases where users do want to use that data as a string, we must
 * allow that to occur.
 *
 * In the future when we have date literals for ingest, we should
 * revert this and only accept JTextT (StrColumn).
 */

trait StringLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait StringLib extends ColumnarTableLib {
    import trans._
    import StdLib.{BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom, StrAndDateT, dateToStrCol}

    val StringNamespace = Vector("std", "string")

    override def _lib1 = super._lib1 ++ Set(length, trim, toUpperCase,
      toLowerCase, isEmpty, intern, parseNum, numToString)

    override def _lib2 = super._lib2 ++ Set(equalsIgnoreCase, codePointAt,
      startsWith, lastIndexOf, concat, endsWith, codePointBefore,
      takeLeft, takeRight, dropLeft, dropRight,
      matches, regexMatch, compareTo, compareToIgnoreCase, compare, compareIgnoreCase,
      equals, indexOf, split, splitRegex, editDistance)

    private def isValidInt(num: BigDecimal): Boolean = {
      try { 
        num.toIntExact; true
      } catch {
        case e: java.lang.ArithmeticException => { false }
      }
    }

    class Op1SS(name: String, f: String => String)
    extends Op1F1(StringNamespace, name) {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)
      private def build(c: StrColumn) = new StrFrom.S(c, _ != null, f)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::str::op1ss::" + name) {
        case c: StrColumn => build(c)
        case c: DateColumn => build(dateToStrCol(c))
      }
    }

    object trim extends Op1SS("trim", _.trim)

    object toUpperCase extends Op1SS("toUpperCase", _.toUpperCase)

    object toLowerCase extends Op1SS("toLowerCase", _.toLowerCase)

    object intern extends Op1SS("intern", _.intern)

    object isEmpty extends Op1F1(StringNamespace, "isEmpty") {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(StrAndDateT, JBooleanT)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::str::isEmpty") {
        case c: StrColumn => new BoolFrom.S(c, _ != null, _.isEmpty)
        case c: DateColumn => new BoolFrom.S(dateToStrCol(c), _ != null, _ => false)
      }
    }

    def neitherNull(x: String, y: String) = x != null && y != null

    object length extends Op1F1(StringNamespace, "length") {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)
      private def build(c: StrColumn) = new LongFrom.S(c, _ != null, _.length)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::str::length") {
        case c: StrColumn => build(c)
        case c: DateColumn => build(dateToStrCol(c))
      }
    }

    class Op2SSB(name: String, f: (String, String) => Boolean)
    extends Op2F2(StringNamespace, name) {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JBooleanT)
      private def build(c1: StrColumn, c2: StrColumn) = new BoolFrom.SS(c1, c2, neitherNull, f)
      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::op2ss" + name) {
        case (c1: StrColumn, c2: StrColumn) => build(c1, c2)
        case (c1: StrColumn, c2: DateColumn) => build(c1, dateToStrCol(c2))
        case (c1: DateColumn, c2: StrColumn) => build(dateToStrCol(c1), c2)
        case (c1: DateColumn, c2: DateColumn) => build(dateToStrCol(c1), dateToStrCol(c2))
      }
    }

    // FIXME: I think it's a bad idea to override Object.equals here...
    object equals extends Op2SSB("equals", _ equals _)

    object equalsIgnoreCase
    extends Op2SSB("equalsIgnoreCase", _ equalsIgnoreCase _)

    object startsWith extends Op2SSB("startsWith", _ startsWith _)

    object endsWith extends Op2SSB("endsWith", _ endsWith _)

    object matches extends Op2SSB("matches", _ matches _)
    
    object regexMatch extends Op2(StringNamespace, "regexMatch") {
      import trans._
      
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JArrayHomogeneousT(JTextT))
      
      def spec[A <: SourceType](ctx: EvaluationContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = {
        trans.Scan(
          trans.InnerArrayConcat(
            trans.WrapArray(left),
            trans.WrapArray(right)),
          scanner)
      }
      
      object scanner extends CScanner {
        type A = Unit
        
        def init = ()
        
        def scan(a: Unit, columns: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
          val targetM = columns get ColumnRef(CPath.Identity \ 0, CString)
          val regexM = columns get ColumnRef(CPath.Identity \ 1, CString)
          
          val columns2M = for (targetRaw <- targetM; regexRaw <- regexM) yield {
            val target: StrColumn = targetRaw match {
              case c: StrColumn => c
              case c: DateColumn => dateToStrCol(c)
              case c => sys.error("didn't expect %s" format c)
            }

            val regex: StrColumn = regexRaw match {
              case c: StrColumn => c
              case c: DateColumn => dateToStrCol(c)
              case c => sys.error("didn't expect %s" format c)
            }
            
            val table = new Array[Array[String]](range.length)
            val defined = new BitSet(range.length)
            var maxLength = 0
            
            RangeUtil.loop(range) { i =>
              if (target.isDefinedAt(i) && regex.isDefinedAt(i)) {
                val str = target(i)
                
                try {
                  val reg = regex(i).r
                  
                  str match {
                    case reg(capture @ _*) => {
                      val capture2 = capture map { str =>
                        if (str == null)
                          ""
                        else
                          str
                      }
                      
                      table(i) = capture2.toArray
                      defined.set(i)
                      maxLength = maxLength max table(i).length
                    }
                    
                    case _ =>
                  }
                } catch {
                  case _: java.util.regex.PatternSyntaxException =>   // yay, scala 
                }
              }
            }
            
            if (maxLength > 0) {
              val pairs = 0 until maxLength map { idx =>
                val col = new StrColumn {
                  def isDefinedAt(row: Int) =
                    defined.get(row) && idx < table(row).length
                  
                  def apply(row: Int) = table(row)(idx)
                }
                
                val ref = ColumnRef(CPath.Identity \ idx, CString)
                
                ref -> col
              }
              
              Map(pairs: _*)
            } else {
              Map[ColumnRef, Column]()
            }
          }
          
          ((), columns2M getOrElse Map[ColumnRef, Column]())
        }
      }
    }

    object concat extends Op2F2(StringNamespace, "concat") {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JTextT)
      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::concat") {
        case (c1: StrColumn, c2: StrColumn) =>
          new StrFrom.SS(c1, c2, neitherNull, _ concat _)
        case (c1: DateColumn, c2: StrColumn) =>
          new StrFrom.SS(dateToStrCol(c1), c2, neitherNull, _ concat _)
        case (c1: StrColumn, c2: DateColumn) =>
          new StrFrom.SS(c1, dateToStrCol(c2), neitherNull, _ concat _)
        case (c1: DateColumn, c2: DateColumn) =>
          new StrFrom.SS(dateToStrCol(c1), dateToStrCol(c2), neitherNull, _ concat _)
      }
    }

    class Op2SLL(name: String,
      defined: (String, Long) => Boolean,
      f: (String, Long) => Long) extends Op2F2(StringNamespace, name) {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, JNumberT, JNumberT)
      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::op2sll::" + name) {
        case (c1: StrColumn, c2: DoubleColumn) =>
          new LongFrom.SD(c1, c2,
            (s, n) => (n % 1 == 0) && defined(s, n.toLong),
            (s, n) => f(s, n.toLong))

        case (c1: StrColumn, c2: LongColumn) =>
          new LongFrom.SL(c1, c2, defined, f)

        case (c1: StrColumn, c2: NumColumn) =>
          new LongFrom.SN(c1, c2,
            (s, n) => (n % 1 == 0) && defined(s, n.toLong),
            (s, n) => f(s, n.toLong))

        case (c1: DateColumn, c2: DoubleColumn) =>
          new LongFrom.SD(dateToStrCol(c1), c2,
            (s, n) => (n % 1 == 0) && defined(s, n.toLong),
            (s, n) => f(s, n.toLong))

        case (c1: DateColumn, c2: LongColumn) =>
          new LongFrom.SL(dateToStrCol(c1), c2, defined, f)

        case (c1: DateColumn, c2: NumColumn) =>
          new LongFrom.SN(dateToStrCol(c1), c2,
            (s, n) => (n % 1 == 0) && defined(s, n.toLong),
            (s, n) => f(s, n.toLong))
      }
    }

    object codePointAt extends Op2SLL("codePointAt",
      (s, n) => n >= 0 && s.length > n,
      (s, n) => s.codePointAt(n.toInt))

    object codePointBefore extends Op2SLL("codePointBefore",
      (s, n) => n >= 0 && s.length > n,
      (s, n) => s.codePointBefore(n.toInt))

    class Substring(name: String)(f: (String, Int) => String) extends Op2F2(StringNamespace, name) {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, JNumberT, JTextT)
      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::substring::" + name) {
        case (c1: StrColumn, c2: LongColumn) =>
          new StrFrom.SL(c1, c2, (s, n) => n >= 0, { (s, n) => f(s, n.toInt) })
        case (c1: StrColumn, c2: DoubleColumn) =>
          new StrFrom.SD(c1, c2, (s, n) => n >= 0 && (n % 1 == 0), { (s, n) => f(s, n.toInt) })
        case (c1: StrColumn, c2: NumColumn) =>
          new StrFrom.SN(c1, c2, (s, n) => n >= 0 && (n % 1 == 0), { (s, n) => f(s, n.toInt) })

        case (c1: DateColumn, c2: LongColumn) =>
          new StrFrom.SL(dateToStrCol(c1), c2, (s, n) => n >= 0, { (s, n) => f(s, n.toInt) })
        case (c1: DateColumn, c2: DoubleColumn) =>
          new StrFrom.SD(dateToStrCol(c1), c2, (s, n) => n >= 0 && (n % 1 == 0), { (s, n) => f(s, n.toInt) })
        case (c1: DateColumn, c2: NumColumn) =>
          new StrFrom.SN(dateToStrCol(c1), c2, (s, n) => n >= 0 && (n % 1 == 0), { (s, n) => f(s, n.toInt) })
      }
    }

    object takeLeft extends Substring("takeLeft")({ (s, n) =>
      s.substring(0, math.min(n, s.length))
    })

    object takeRight extends Substring("takeRight")({ (s, n) =>
      s.substring(math.max(s.length - n, 0))
    })

    object dropLeft extends Substring("dropLeft")({ (s, n) =>
      s.substring(math.min(n, s.length))
    })

    object dropRight extends Substring("dropRight")({ (s, n) =>
      s.substring(0, math.max(0, s.length - n))
    })

    class Op2SSL(name: String, f: (String, String) => Long)
    extends Op2F2(StringNamespace, name) {
      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JNumberT)
      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::op2ssl::" + name) {
        case (c1: StrColumn, c2: StrColumn) =>
          new LongFrom.SS(c1, c2, neitherNull, f)
        case (c1: DateColumn, c2: StrColumn) =>
          new LongFrom.SS(dateToStrCol(c1), c2, neitherNull, f)
        case (c1: StrColumn, c2: DateColumn) =>
          new LongFrom.SS(c1, dateToStrCol(c2), neitherNull, f)
        case (c1: DateColumn, c2: DateColumn) =>
          new LongFrom.SS(dateToStrCol(c1), dateToStrCol(c2), neitherNull, f)
      }
    }

    object compareTo extends Op2SSL("compareTo", _ compareTo _) {
      override val deprecation = Some("use compare instead")
    }

    object compare extends Op2SSL("compare", _ compareTo _)

    object compareToIgnoreCase extends Op2SSL("compareToIgnoreCase",
      _ compareToIgnoreCase _) {
      override val deprecation = Some("use compareIgnoreCase instead")
    }

    object compareIgnoreCase extends Op2SSL("compareIgnoreCase",
      _ compareToIgnoreCase _)

    object indexOf extends Op2SSL("indexOf", _ indexOf _)

    object lastIndexOf extends Op2SSL("lastIndexOf", _ lastIndexOf _)

    object parseNum extends Op1F1(StringNamespace, "parseNum") {
      import java.util.regex.Pattern

      val intPattern = Pattern.compile("^-?(?:0|[1-9][0-9]*)$")
      val decPattern = Pattern.compile("^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][-+]?[0-9]+)?$")

      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)

      private def build(c: StrColumn) = new Map1Column(c) with NumColumn {
        override def isDefinedAt(row: Int): Boolean = {
          if (!super.isDefinedAt(row)) return false
          val s = c(row)
          s != null && decPattern.matcher(s).matches
        }

        def apply(row: Int) = BigDecimal(c(row))
      }

      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::str::parseNum") {
        case c: StrColumn => build(c)
        case c: DateColumn => build(dateToStrCol(c))
      }
    }

    object numToString extends Op1F1(StringNamespace, "numToString") {
      val tpe = UnaryOperationType(JNumberT, JTextT)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::str::numToString") {
        case c: LongColumn => new StrFrom.L(c, _ => true, _.toString)
        case c: DoubleColumn => {
          new StrFrom.D(c, _ => true, { d =>
            val back = d.toString
            if (back.endsWith(".0"))
              back.substring(0, back.length - 2)
            else
              back
          })
        }
        case c: NumColumn => new StrFrom.N(c, _ => true, _.toString)
      }
    }

    object split extends Op2F2(StringNamespace, "split") {
      import java.util.regex.Pattern

      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JArrayHomogeneousT(JTextT))

      private def build(c1: StrColumn, c2: StrColumn) = new Map2Column(c1, c2) with HomogeneousArrayColumn[String] {
        val tpe = CArrayType(CString)
        override def isDefinedAt(row: Int): Boolean =
          super.isDefinedAt(row) && c1.isDefinedAt(row) && c2.isDefinedAt(row)

        def apply(row: Int): Array[String] =
          Pattern.compile(Pattern.quote(c2(row))).split(c1(row), -1)
      }

      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::split") {
        case (c1: StrColumn, c2: StrColumn) => build(c1, c2)
        case (c1: DateColumn, c2: StrColumn) => build(dateToStrCol(c1), c2)
        case (c1: StrColumn, c2: DateColumn) => build(c1, dateToStrCol(c2))
        case (c1: DateColumn, c2: DateColumn) => build(dateToStrCol(c1), dateToStrCol(c2))
      }
    }

    object editDistance extends Op2F2(StringNamespace, "editDistance") {
      import java.util.regex.Pattern

      val intPattern = Pattern.compile("^-?(?:0|[1-9][0-9]*)$")
      val decPattern = Pattern.compile("^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][-+]?[0-9]+)?$")

      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JNumberT)

      private def build(c1: StrColumn, c2: StrColumn) =
        new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int): Long = Levenshtein.distance(c1(row), c2(row))
        }

      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::parseNum") {
        case (c1: StrColumn, c2: StrColumn) => build(c1, c2)
        case (c1: DateColumn, c2: StrColumn) => build(dateToStrCol(c1), c2)
        case (c1: StrColumn, c2: DateColumn) => build(c1, dateToStrCol(c2))
        case (c1: DateColumn, c2: DateColumn) => build(dateToStrCol(c1), dateToStrCol(c2))
      }
    }

    object splitRegex extends Op2F2(StringNamespace, "splitRegex") {
      import java.util.regex.Pattern

      //@deprecated, see the DEPRECATED comment in StringLib
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JArrayHomogeneousT(JTextT))

      private def build(c1: StrColumn, c2: StrColumn) = new Map2Column(c1, c2) with HomogeneousArrayColumn[String] {
        val tpe = CArrayType(CString)
        override def isDefinedAt(row: Int): Boolean = {
          try {
            if (super.isDefinedAt(row) && c1.isDefinedAt(row) && c2.isDefinedAt(row)) {
              Pattern.compile(c2(row))
              true
            } else {
              false
            }
          } catch {
            case _: Exception => false
          }
        }

        def apply(row: Int): Array[String] =
          Pattern.compile(c2(row)).split(c1(row), -1)
      }

      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::str::splitRegex") {
        case (c1: StrColumn, c2: StrColumn) => build(c1, c2)
        case (c1: DateColumn, c2: StrColumn) => build(dateToStrCol(c1), c2)
        case (c1: StrColumn, c2: DateColumn) => build(c1, dateToStrCol(c2))
        case (c1: DateColumn, c2: DateColumn) => build(dateToStrCol(c1), dateToStrCol(c2))
      }
    }
  }
}
