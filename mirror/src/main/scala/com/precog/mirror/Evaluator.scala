package com.precog
package mirror

import quirrel._
import quirrel.typer._
import quirrel.emitter._
import util.IdGen

import blueeyes.json._

import scalaz.Ordering
import scalaz.syntax.semigroup._

import scala.collection.mutable

trait EvaluatorModule extends ProvenanceChecker
    with Binder
    with GroupSolver
    with Compiler
    with LineErrors
    with LibraryModule {
      
  import ast._
  
  private type Identities = Vector[Int]
  private type Dataset = Seq[(Identities, JValue)]
  
  // TODO more specific sequence types
  def eval(expr: Expr)(fs: String => Seq[JValue]): Seq[JValue] = {
    val inits = mutable.Map[String, Int]()
    val news = mutable.Map[New, Int]()
    val IdGen = new IdGen
    
    def mappedFS(path: String): Seq[(Identities, JValue)] = {
      val raw = fs(path)
      
      val init = inits get path getOrElse {
        val init = IdGen.nextInt()
        inits += (path -> init)
        
        0 until raw.length foreach { _ => IdGen.nextInt() }     // ew....
        
        init
      }
      
      // done in this order for eagerness reasons
      raw zip (Stream from init map { Vector(_) }) map { case (a, b) => (b, a) }
    }
    
    def loop(env: Map[(Let, String), Dataset])(expr: Expr): Dataset = expr match {
      case Let(loc, _, _, _, right) => loop(env)(right)
      
      case Solve(loc, constraints, child) =>
        sys.error("todo")
      
      case Import(_, _, child) => loop(env)(child)
      
      case Assert(loc, pred, child) => {
        val result = loop(env)(pred) forall {
          case (_, JBool(b)) => b
          case _ => true
        }
        
        if (result)
          loop(env)(child)
        else
          throw new RuntimeException("assertion failed: %d:%d".format(loc.lineNum, loc.colNum))
      }
      
      case Observe(_, _, _) => sys.error("todo")
      
      case expr @ New(_, child) => {
        val raw = loop(env)(child) map { case (_, v) => v }
        
        val init = news get expr getOrElse {
          val init = IdGen.nextInt()
          news += (expr -> init)
          
          0 until raw.length foreach { _ => IdGen.nextInt() }     // ew....
          
          init
        }
        
        // done in this order for eagerness reasons
        raw zip (Stream from init map { Vector(_) }) map { case (a, b) => (b, a) }
      }
      
      // TODO add support for scoped constraints
      case Relate(_, _, _, in) => loop(env)(in)
      
      case TicVar(_, _) => sys.error("todo")
      
      case StrLit(_, value) => (Vector(), JString(value)) :: Nil
      
      case NumLit(_, value) => (Vector(), JNumBigDec(BigDecimal(value))) :: Nil
      
      case BoolLit(_, true) => (Vector(), JTrue) :: Nil
      case BoolLit(_, false) => (Vector(), JFalse) :: Nil
      
      case UndefinedLit(_) => Nil
      
      case NullLit(_) => (Vector(), JNull) :: Nil
      
      case ObjectDef(loc, props) => {
        val propResults = props map {
          case (name, expr) =>
            (name, loop(env)(expr), expr.provenance)
        }
        
        val wrappedResults = propResults map {
          case (name, data, prov) => {
            val mapped = data map {
              case (ids, v) => (ids, JObject(Map(name -> v)): JValue)
            }
            
            (mapped, prov)
          }
        }
        
        val resultOpt = wrappedResults.reduceLeftOption[(Dataset, Provenance)]({
          case ((left, leftProv), (right, rightProv)) => {
            val back = handleBinary(left, leftProv, right, rightProv) {
              case (JObject(leftFields), JObject(rightFields)) =>
                JObject(leftFields ++ rightFields)
            }
            
            // would have failed to type check otherwise
            val prov = unifyProvenance(expr.relations)(leftProv, rightProv).get
            
            (back, prov)
          }
        })
        
        resultOpt map { case (data, _) => data } getOrElse {
          (Vector(), JArray(Nil)) :: Nil
        }
      }
      
      case ArrayDef(loc, values) => {
        val valueResults = values map { expr =>
          (loop(env)(expr), expr.provenance)
        }
        
        val wrappedValues = valueResults map {
          case (data, prov) => {
            val mapped = data map {
              case (ids, v) => (ids, JArray(v :: Nil): JValue)
            }
            
            (mapped, prov)
          }
        }
        
        val resultOpt = wrappedValues.reduceLeftOption[(Dataset, Provenance)]({
          case ((left, leftProv), (right, rightProv)) => {
            val back = handleBinary(left, leftProv, right, rightProv) {
              case (JArray(leftValues), JArray(rightValues)) =>
                JArray(leftValues ++ rightValues)
            }
            
            // would have failed to type check otherwise
            val prov = unifyProvenance(expr.relations)(leftProv, rightProv).get
            
            (back, prov)
          }
        })
        
        resultOpt map { case (data, _) => data } getOrElse {
          (Vector(), JArray(Nil)) :: Nil
        }
      }
      
      case Descent(loc, child, property) => {
        loop(env)(child) collect {
          case (ids, value: JObject) => (ids, value \ property)
        }
      }
      
      case MetaDescent(_, _, _) => sys.error("todo")
      
      case Deref(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JArray(values), JNum(index)) =>
            values(index.toInt)
        }
      }
      
      case expr @ Dispatch(loc, Identifier(ns, id), actuals) => {
        val actualSets = actuals map loop(env)
        
        expr.binding match {
          case LetBinding(b) => {
            val env2 = env ++ ((Stream continually b) zip b.params zip actualSets)
            loop(env2)(b.left)
          }
          
          case FormalBinding(b) => env((b, id))
          
          case LoadBinding => {
            actualSets.head collect {
              case (_, JString(path)) => mappedFS(path)
            } flatten
          }
          
          case ExpandGlobBinding => actualSets.head       // TODO
          
          case _ => sys.error("todo")
        }
      }
      
      case Cond(_, pred, left, right) => sys.error("todo")
      
      case Where(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (value, JTrue) => value
        }
      }
      
      case With(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JObject(leftFields), JObject(rightFields)) =>
            JObject(leftFields ++ rightFields)
        }
      }
      
      case Union(_, _, _) => sys.error("todo")
      case Intersect(_, _, _) => sys.error("todo")
      case Difference(_, _, _) => sys.error("todo")
      
      case Add(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN + rightN)
        }
      }
      
      case Sub(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN - rightN)
        }
      }
      
      case Mul(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN * rightN)
        }
      }
      
      case Div(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN / rightN)
        }
      }
      
      case Mod(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN % rightN)
        }
      }
      
      case Pow(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN pow rightN.toInt)
        }
      }
      
      case Lt(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN < rightN)
        }
      }
      
      case LtEq(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN <= rightN)
        }
      }
      
      case Gt(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN > rightN)
        }
      }
      
      case GtEq(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN >= rightN)
        }
      }
      
      case Eq(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (leftV, rightV) => JBool(leftV == rightV)
        }
      }
      
      case NotEq(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (leftV, rightV) => JBool(leftV != rightV)
        }
      }
      
      case And(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JBool(leftB), JBool(rightB)) => JBool(leftB && rightB)
        }
      }
      
      case Or(_, left, right) => {
        handleBinary(loop(env)(left), left.provenance, loop(env)(right), right.provenance) {
          case (JBool(leftB), JBool(rightB)) => JBool(leftB || rightB)
        }
      }
      
      case Comp(_, child) => {
        loop(env)(child) collect {
          case (ids, JBool(value)) => (ids, JBool(!value))
        }
      }
      
      case Neg(_, child) => {
        loop(env)(child) collect {
          case (ids, JNum(value)) => (ids, JNum(-value))
        }
      }
      
      case Paren(_, child) => loop(env)(child)
    }
    
    def handleBinary(left: Dataset, leftProv: Provenance, right: Dataset, rightProv: Provenance)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      val intersected = leftProv.possibilities intersect rightProv.possibilities filter { p => p != ValueProvenance && p != NullProvenance }
      
      if (intersected.isEmpty) {
        // perform a cartesian
        cross(left, right)(pf)
      } else {
        // perform a join
        join(left, leftProv, right, rightProv)(pf)
      }
    }
    
    def join(left: Dataset, leftProv: Provenance, right: Dataset, rightProv: Provenance)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      // TODO compute join keys
      val indicesLeft = List(0)
      val indicesRight = List(0)
      
      // TODO compute merge key
      val mergeKey: List[Either[Int, Int]] = List(Left(0))
      
      val joined = zipAlign(left, right) {
        case ((idsLeft, _), (idsRight, _)) => {
          val zipped = (indicesLeft map idsLeft) zip (indicesRight map idsRight)
          
          zipped map {
            case (x, y) => Ordering.fromInt(x - y)
          } reduce { _ |+| _ }
        }
      }
      
      joined collect {
        case ((idsLeft, leftV), (idsRight, rightV)) if pf.isDefinedAt((leftV, rightV)) => {
          val idsMerged = mergeKey map {
            case Left(i) => idsLeft(i)
            case Right(i) => idsRight(i)
          }
          
          (Vector(idsMerged: _*), pf(leftV, rightV))
        }
      }
    }
    
    def cross(left: Dataset, right: Dataset)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      for {
        (idsLeft, leftV) <- left
        (idsRight, rightV) <- right
        
        if pf.isDefinedAt((leftV, rightV))
      } yield (idsLeft ++ idsRight, pf((leftV, rightV)))
    }
    
    if (expr.errors.isEmpty) {
      loop(Map())(expr) map {
        case (_, value) => value
      }
    } else {
      Seq.empty
    }
  }
  
  /**
   * Poor-man's cogroup specialized on the middle case
   */
  private def zipAlign[A, B](left: Seq[A], right: Seq[B])(f: (A, B) => Ordering): Seq[(A, B)] = {
    if (left.isEmpty || right.isEmpty) {
      Nil
    } else {
      f(left.head, right.head) match {
        case Ordering.EQ => (left.head, right.head) +: zipAlign(left.tail, right.tail)(f)
        case Ordering.LT => zipAlign(left.tail, right)(f)
        case Ordering.GT => zipAlign(left, right.tail)(f)
      }
    }
  }
}
