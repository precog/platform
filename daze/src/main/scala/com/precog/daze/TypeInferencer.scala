package com.precog
package daze

import yggdrasil._

trait TypeInferencer extends DAG {
  import instructions.{ DerefArray, DerefObject, Map2Cross, Map2CrossLeft, Map2CrossRight }
  import dag._

  def inferTypes(jtpe: JType)(graph: DepGraph) : DepGraph = {
    def inferSplitTypes(split: Split) = split match {
      case Split(loc, spec, child) => Split(loc, spec, inferTypes(jtpe)(child))
    }

    graph match {
      case r : Root => r

      case New(loc, parent) => New(loc, inferTypes(jtpe)(parent))

      case LoadLocal(loc, parent, _) => LoadLocal(loc, parent, jtpe)

      case Operate(loc, op, parent) => Operate(loc, op, inferTypes(jtpe)(parent))

      case Reduce(loc, red, parent) => Reduce(loc, red, inferTypes(jtpe)(parent))

      case Morph1(loc, m, parent) => Morph1(loc, m, inferTypes(jtpe)(parent))

      case Morph2(loc, m, left, right) => Morph2(loc, m, inferTypes(jtpe)(left), inferTypes(jtpe)(right))

      case Join(loc, instr, left, right) => (instr, right.value) match {
        case (Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), Some(SString(str))) =>
          Join(loc, instr, inferTypes(JObjectFixedT(Map(str -> jtpe)))(left), right)

        case (Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), Some(SDecimal(d))) =>
          Join(loc, instr, inferTypes(JArrayFixedT(Map(d.toInt -> jtpe)))(left), right)

        case _ => Join(loc, instr, inferTypes(jtpe)(left), inferTypes(jtpe)(right))
      }

      case Filter(loc, cross, target, boolean) => Filter(loc, cross, inferTypes(jtpe)(target), inferTypes(jtpe)(boolean))

      case Sort(parent, indices) => Sort(inferTypes(jtpe)(parent), indices)

      case Memoize(parent, priority) => Memoize(inferTypes(jtpe)(parent), priority)

      case Distinct(loc, parent) => Distinct(loc, inferTypes(jtpe)(parent))

      case s : Split => inferSplitTypes(s)

      case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(inferSplitTypes(s.parent))

      case s @ SplitParam(loc, id) => SplitParam(loc, id)(inferSplitTypes(s.parent))
    }
  }
}
