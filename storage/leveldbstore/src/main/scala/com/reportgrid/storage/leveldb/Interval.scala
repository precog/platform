package reportgrid.storage.leveldb

case class Interval[T: Ordering](start : Option[T], end : Option[T]) {
  import scala.math.Ordered._
  def withStart(start: Option[T]) = Interval(start, end)
  def withEnd(end: Option[T]) = Interval(start, end)

  def startsBefore(other: Interval[T]): Boolean = { 
    start.forall(s => other.start.exists(_ > s)) 
  }

  def endsAfter(other: Interval[T]): Boolean = { 
    end.forall(e => other.end.exists(_ < e)) 
  }

  def disjoint(other: Interval[T]): Boolean = { 
    end.flatMap(x => other.start.filter(_ > x)).
    orElse(start.flatMap(x => other.end.filter(_ < x))).
    isDefined
  }

  def intersect(other: Interval[T]): Option[Interval[T]] = { 
    if (disjoint(other)) {
      None
    } else if (startsBefore(other) && endsAfter(other)) {
      Some(other)
    } else if (startsBefore(other) && other.endsAfter(this)) {
      Some(withEnd(other.start))
    } else if (other.startsBefore(this) && endsAfter(other)) {
      Some(withStart(other.end))
    } else {
      other.intersect(this);
    }   
  }

  def union(other: Interval[T]): Set[Interval[T]] = { 
    if (disjoint(other)) {
      Set(this, other)
    } else if (startsBefore(other) && endsAfter(other)) {
      Set(this)
    } else if (startsBefore(other) && other.endsAfter(this)) {
      Set(withEnd(other.end))
    } else if (other.startsBefore(this) && endsAfter(other)) {
      Set(withStart(other.start))
    } else {
      other.union(this)
    }   
  }
}

// vim: set ts=4 sw=4 et:
