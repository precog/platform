package com.precog.yggdrasil

import org.specs2.mutable._
import org.specs2.matcher.MatchResult
import scalaz._
import scalaz.Either3._

object TableSpec extends Specification {
  "a table" should {
    "cogroup" in {
      "a static full dataset" >> {
        val v1 = new TestTable(
          List(Array(0L, 1L, 3L, 3L, 5L, 7L, 8L, 8L)),
          Map(CMeta(CDyn(0), CLong) -> Array(0L, 1L, 3L, 3L, 5L, 7L, 8L, 8L))
        )

        val v2 = new TestTable(
          List(Array(0L, 2L, 3L, 4L, 5L, 5L, 6L, 8L, 8L)),
          Map(CMeta(CDyn(1), CLong) -> Array(0L, 2L, 3L, 4L, 5L, 5L, 6L, 8L, 8L))
        )

        val expected = Vector(
          middle3((0L, 0L)),
          left3(1L),
          right3(2L),
          middle3((3L, 3L)),
          middle3((3L, 3L)),
          right3(4L),
          middle3((5L, 5L)),
          middle3((5L, 5L)),
          right3(6L),
          left3(7L),
          middle3((8L, 8L)),
          middle3((8L, 8L)),
          middle3((8L, 8L)),
          middle3((8L, 8L)) 
        )

        val results = v1.cogroup(v2) {
          new Table.CogroupF {
            def one = Map()
            def both = Map()
          }
        }

        val rowView = results.rowView

        (rowView.state must_== RowView.BeforeStart) and 
        (rowView.advance must_== RowView.Data) and 
        expected.foldLeft(ok: MatchResult[Any]) {
          case (result, e @ Left3(v)) =>
            result and (rowView.idCount must_== 1) and
            (rowView.columns must_== Set(CMeta(CDyn(0), CLong))) and
            //(rowView.valueAt(CMeta(CDyn(0), CLong)) must_== v) and
            (rowView.advance must beLike {
              case RowView.Data => ok
              case RowView.AfterEnd => rowView.advance must_== RowView.AfterEnd
            }) 

          case (result, e @Middle3((l, r))) =>
            result and (rowView.idCount must_== 1) and
            (rowView.columns must_== Set(CMeta(CDyn(0), CLong), CMeta(CDyn(1), CLong))) and
            //(rowView.valueAt(CMeta(CDyn(0), CLong)) must_== l) and
            //(rowView.valueAt(CMeta(CDyn(1), CLong)) must_== r) and
            (rowView.advance must beLike {
              case RowView.Data => ok
              case RowView.AfterEnd => rowView.advance must_== RowView.AfterEnd
            }) 

          case (result, e @Right3(v)) =>
            result and (rowView.idCount must_== 1) and
            (rowView.columns must_== Set(CMeta(CDyn(1), CLong))) and
            //(rowView.valueAt(CMeta(CDyn(1), CLong)) must_== v) and
            (rowView.advance must beLike {
              case RowView.Data => ok
              case RowView.AfterEnd => rowView.advance must_== RowView.AfterEnd
            }) 
        }
      }

      /*
      "a static single pair dataset" in {
        // Catch bug where equal at the end of input produces middle, right
        val v1s = IterableDataset(1, Vector(rec(0)))
        val v2s = IterableDataset(1, Vector(rec(0)))

        val expected2 = Vector(middle3((0, 0)))

        val results2 = v1s.cogroup(v2s) {
          new CogroupF[Long, Long, Either3[Long, (Long, Long), Long]] {
            def left(i: Long) = left3(i)
            def both(i1: Long, i2: Long) = middle3((i1, i2))
            def right(i: Long) = right3(i)
          }
        }

        Vector(results2.iterator.toSeq: _*) mustEqual expected2
      }

      */
    }
  }

}

class TestTable(ids: List[Array[Long]], values: Map[CMeta, Array[_]]) extends Table { table =>
  def idCount = ids.size

  def rowView = new RowView {
    type Position = Int

    private var pos = -1
    private var _state = RowView.BeforeStart

    def position = pos
    def state = _state
    def advance = {
      pos += 1
      if (ids.forall(l => pos < l.length)) RowView.Data else RowView.AfterEnd
    }

    def reset(newPos: Position) = {
      pos = newPos
      if (pos < 0) RowView.BeforeStart
      else if (ids.forall(l => pos < l.length)) RowView.Data
      else RowView.AfterEnd
    }

    
    protected[yggdrasil] def idCount: Int = table.idCount
    protected[yggdrasil] def columns: Set[CMeta] = table.values.keySet

    protected[yggdrasil] def idAt(i: Int): Identity = ids(i)(pos)
    protected[yggdrasil] def hasValue(meta: CMeta): Boolean = {
      pos >= 0 && table.values.contains(meta) && table.values(meta).length > pos
    }
    protected[yggdrasil] def valueAt(meta: CMeta): Any = table.values(meta)(pos)
  }
}

// vim: set ts=4 sw=4 et:
