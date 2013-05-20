package com.precog.util

/**
 * This class exists as a replacement for Unit in Unit-returning functions.
 * The main issue with unit is that the coercion of any return value to
 * unit means that we were sometimes masking mis-returns of functions. In
 * particular, functions returning IO[Unit] would happily coerce IO => Unit,
 * which essentially discarded the inner IO work.
 */
sealed trait PrecogUnit

object PrecogUnit extends PrecogUnit {
  implicit def liftUnit(unit: Unit): PrecogUnit = this
}
