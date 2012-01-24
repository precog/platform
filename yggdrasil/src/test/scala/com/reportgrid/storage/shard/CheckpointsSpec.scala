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
package com.reportgrid.storage.shard

import com.reportgrid.yggdrasil.shard.CheckpointMetadata

import org.specs2.mutable.Specification

import scalaz.{Success, Failure}

class CheckpointsSpec extends Specification {
  
  "Checkpoints" should {
    "accept a map as input state" in {
      val c = new CheckpointMetadata
      val in = Map() + (1 -> 123) + (2 -> 456)
     
      c.load(in)
      
      c.toMap must_== in
    }
    "adds state for single event expectation" in {
      val c = new CheckpointMetadata

      c.expect(3,2,1)
      c.event(3,2)

      c.toMap must_== Map() + (3 -> 2)
    }
    "does not adjust state before multi event expectation completed" in {
      val c = new CheckpointMetadata

      c.expect(1,2,3)
      c.event(1,2)
      c.event(1,2)

      c.toMap must_== Map.empty
    }
    "updates state for completed multi event expectation" in {
      val c = new CheckpointMetadata

      c.expect(1,2,3)
      c.event(1,2)
      c.event(1,2)
      c.event(1,2)

      c.toMap must_== Map() + (1 -> 2)
    }
    "reset clears public and hidden state" in {
      val c = new CheckpointMetadata

      c.expect(1,2,1)
      c.expect(2,3,2)

      c.event(1,2)
      c.event(2,3)

      c.reset

      c.toMap must_== Map.empty

      c.event(2,3)

      c.toMap must_== Map.empty
    }
    "event updates return success and remaining event count" in {
      val c = new CheckpointMetadata

      c.expect(1,2,3)

      c.event(1,2) must beLike {
        case Success(2) => ok
      }
      
      c.event(1,2) must beLike {
        case Success(1) => ok
      }
      
      c.event(1,2) must beLike {
        case Success(0) => ok
      }
      
      c.event(1,2) must beLike {
        case Failure(_) => ok
      }
    }
    "event without expectation returns error" in {
      val c = new CheckpointMetadata

      c.event(1,2) must beLike {
        case Failure(_) => ok
      }
    }
    "non postive expectation count fails fast" in {
      val c = new CheckpointMetadata

      c.expect(1,2,0) must beLike {
        case Failure(_) => ok  
      }

      c.toMap must_== Map.empty
    }
    "redundant expectation fails fast" in {
      val c = new CheckpointMetadata

      c.expect(1,2,1) must beLike {
        case Success(()) => ok
      }

      c.expect(1,2,4) must beLike {
        case Failure(_) => ok
      }

      c.toMap must_== Map.empty

      c.event(1,2)

      c.toMap must_== Map() + (1 -> 2) 
    }
  }
}
