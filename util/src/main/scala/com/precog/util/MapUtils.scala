package com.precog.util

import scala.collection.{GenTraversable, GenMap}
import scala.collection.generic.CanBuildFrom

import scalaz.{Either3, Left3, Right3, Middle3}

/**
 * Implicit container trait
 */
trait MapUtils {
  implicit def pimpMapUtils[A, B, CC[B] <: GenTraversable[B]](self: GenMap[A, CC[B]]): MapPimp[A, B, CC] =
    new MapPimp(self)
}

class MapPimp[A, B, CC[B] <: GenTraversable[B]](left: GenMap[A, CC[B]]) {
  def cogroup[C, CC2[C] <: GenTraversable[C], Result](right: GenMap[A, CC2[C]])(implicit cbf: CanBuildFrom[GenMap[A, _], Either3[B, (CC[B], CC2[C]), C], Result], cbfLeft: CanBuildFrom[CC[B], B, CC[B]], cbfRight: CanBuildFrom[CC2[C], C, CC2[C]]): Result = {
    val resultBuilder = cbf()
    
    left foreach {
      case (key, leftValues) => {
        right get key map { rightValues =>
          resultBuilder += Either3.middle3[B, (CC[B], CC2[C]), C]((leftValues, rightValues))
        } getOrElse {
          leftValues foreach { b => resultBuilder += Either3.left3[B, (CC[B], CC2[C]), C](b) }
        }
      }
    }
    
    right foreach {
      case (key, rightValues) => {
        if (!(left get key isDefined)) {
          rightValues foreach { c => resultBuilder += Either3.right3[B, (CC[B], CC2[C]), C](c) }
        }
      }
    }
    
    resultBuilder.result()
  }
}
