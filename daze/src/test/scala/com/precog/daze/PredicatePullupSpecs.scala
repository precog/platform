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
package com.precog
package daze

import com.precog.common._
import com.precog.util.Identifier
import org.specs2.execute.Result
import org.specs2.mutable.Specification

import yggdrasil._
import yggdrasil.test._

trait PredicatePullupSpecs[M[+_]] extends Specification with EvaluatorTestSupport[M] {
  import dag._
  import library._

  val ctx = defaultEvaluationContext
  
  "Predicate pullups optimization" should {
    "pull a predicate out of a solve with a single ticvar" in {
      val rawInput = """
        | clicks := //clicks
        |
        | upperBound := 1329643873628
        | lowerBound := 1328866273610
        | extraLB := lowerBound - (24*60*60000)
        |
        | solve 'userId
        |   clicks' := clicks where clicks.time <= upperBound & clicks.time >= extraLB & clicks.userId = 'userId
        |   {userId: 'userId, time: clicks'.time}
        | """.stripMargin

      val loc = instructions.Line(1, 1, "")
      
      val load = LoadLocal(Const(CString("/clicks"))(loc))(loc)
      
      val id = new Identifier
      
      val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              IntersectBucketSpec(
                Extra(
                  Join(instructions.LtEq,CrossLeftSort,
                    Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
                    Const(CLong(1329643873628L))(loc)
                  )(loc)
                ),
                Extra(
                  Join(instructions.GtEq,CrossLeftSort,
                    Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
                    Const(CLong(1328779873610L))(loc)
                  )(loc)
                )
              ),
              UnfixedSolution(1,
                Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("userId"))(loc))(loc)
              )
            )
          ),
          Join(instructions.JoinObject,CrossLeftSort,
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("userId"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("time"))(loc),
              Join(instructions.DerefObject,CrossLeftSort,
                SplitGroup(0, load.identities, id)(loc),
                Const(CString("time"))(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)
        
      val filteredLoad =
        Filter(IdentitySort,
          load,
          Join(instructions.And,IdentitySort,
            Join(instructions.LtEq,CrossLeftSort,
              Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
              Const(CLong(1329643873628L))(loc)
            )(loc),
            Join(instructions.GtEq,CrossLeftSort,
              Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
              Const(CLong(1328779873610L))(loc)
            )(loc)
          )(loc)
        )(loc)
            
      val expected =
        Split(
          Group(0,
            filteredLoad,
            UnfixedSolution(1,
              Join(instructions.DerefObject,CrossLeftSort,
                filteredLoad,
                Const(CString("userId"))(loc)
              )(loc)
            )
          ),
          Join(instructions.JoinObject,CrossLeftSort,
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("userId"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("time"))(loc),
              Join(instructions.DerefObject,CrossLeftSort,
                SplitGroup(0, load.identities, id)(loc),
                Const(CString("time"))(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)

      predicatePullups(split, ctx) mustEqual expected
    }

    "pull a predicate out of a solve with more than one ticvar" in {
      val rawInput = """
        | clicks := //clicks
        |
        | upperBound := 1329643873628
        | lowerBound := 1328866273610
        | extraLB := lowerBound - (24*60*60000)
        |
        | solve 'userId, 'pageId
        |   clicks' := clicks where clicks.time <= upperBound & clicks.time >= extraLB & clicks.userId = 'userId & clicks.pageId = 'pageId
        |   {userId: 'userId, time: clicks'.time}
        | """.stripMargin
        
      val id = new Identifier

      val loc = instructions.Line(1, 1, "")

      val load = LoadLocal(Const(CString("/clicks"))(loc))(loc) 
      
      val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              IntersectBucketSpec(
                IntersectBucketSpec(
                  Extra(
                    Join(instructions.LtEq,CrossLeftSort,
                      Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
                      Const(CLong(1329643873628L))(loc)
                    )(loc)
                  ),
                  Extra(
                    Join(instructions.GtEq,CrossLeftSort,
                      Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
                      Const(CLong(1328779873610L))(loc)
                    )(loc)
                  )
                ),
                UnfixedSolution(1,
                  Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("userId"))(loc))(loc)
                )
              ),
              UnfixedSolution(2,
                Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("pageId"))(loc))(loc)
              )
            )
          ),
          Join(instructions.JoinObject,CrossLeftSort,
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("userId"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("time"))(loc),
              Join(instructions.DerefObject,CrossLeftSort,
                SplitGroup(0, load.identities, id)(loc),
                Const(CString("time"))(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)
        
        val filteredLoad =
          Filter(IdentitySort,
            load,
            Join(instructions.And,IdentitySort,
              Join(instructions.LtEq,CrossLeftSort,
                Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
                Const(CLong(1329643873628L))(loc)
              )(loc),
              Join(instructions.GtEq,CrossLeftSort,
                Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("time"))(loc))(loc),
                Const(CLong(1328779873610L))(loc)
              )(loc)
            )(loc)
          )(loc)
              
        val expected = 
          Split(
            Group(0,
              filteredLoad,
              IntersectBucketSpec(
                UnfixedSolution(1,
                  Join(instructions.DerefObject,CrossLeftSort,
                    filteredLoad,
                    Const(CString("userId"))(loc)
                  )(loc)
                ),
                UnfixedSolution(2,
                  Join(instructions.DerefObject,CrossLeftSort,
                    filteredLoad,
                    Const(CString("pageId"))(loc)
                  )(loc)
                )
              )
            ),
            Join(instructions.JoinObject,CrossLeftSort,
              Join(instructions.WrapObject,CrossLeftSort,
                Const(CString("userId"))(loc),
                SplitParam(1, id)(loc)
              )(loc),
              Join(instructions.WrapObject,CrossLeftSort,
                Const(CString("time"))(loc),
                Join(instructions.DerefObject,CrossLeftSort,
                  SplitGroup(0, load.identities, id)(loc),
                  Const(CString("time"))(loc)
                )(loc)
              )(loc)
            )(loc), id
          )(loc)

      predicatePullups(split, ctx) mustEqual expected
    }

    "pull a predicate out of the top level of a nested solve" in {
      val rawInput = """
        | medals := //summer_games/london_medals
        | 
        | solve 'gender
        |   medals' := medals where medals.Gender = 'gender & medals.Edition = 2000
        | 
        |   solve 'weight
        |     medals' where medals'.Weight = 'weight
        | """.stripMargin
        
      val id1 = new Identifier
      val id2 = new Identifier
        
      val loc = instructions.Line(1, 1, "")
      
      val load = LoadLocal(Const(CString("/summer_games/london_medals"))(loc))(loc)
      
      lazy val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              UnfixedSolution(1,
                Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("Gender"))(loc))(loc)
              ),
              Extra(
                Join(instructions.Eq,CrossLeftSort,
                  Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("Edition"))(loc))(loc),
                  Const(CLong(2000))(loc)
                )(loc)
              )
            )
          ),
          innerSplit,
          id1
        )(loc)
        
      lazy val innerSplit =
        Split(
          Group(2,
            SplitGroup(0, load.identities, id1)(loc),
            UnfixedSolution(3,
              Join(instructions.DerefObject,CrossLeftSort,
                SplitGroup(0, load.identities, id1)(loc),
                Const(CString("Weight"))(loc)
              )(loc)
            )
          ),
          SplitGroup(2, load.identities, id2)(loc),
          id2
        )(loc)
      
      val filteredLoad =
        Filter(IdentitySort,
          load,
          Join(instructions.Eq,CrossLeftSort,
            Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("Edition"))(loc))(loc),
            Const(CLong(2000))(loc)
          )(loc)
        )(loc)

      lazy val expected =  
        Split(
          Group(0,
            filteredLoad,
            UnfixedSolution(1,
              Join(instructions.DerefObject,CrossLeftSort,
                filteredLoad,
                Const(CString("Gender"))(loc)
              )(loc)
            )
          ),
          expectedInner,
          id1
        )(loc)
        
      lazy val expectedInner =  
        Split(
          Group(2,
            SplitGroup(0,
              load.identities,
              id1
            )(loc),
            UnfixedSolution(3,
              Join(instructions.DerefObject,CrossLeftSort,
                SplitGroup(0, load.identities, id1)(loc),
                Const(CString("Weight"))(loc)
              )(loc)
            )
          ),
          SplitGroup(2, load.identities, id2)(loc),
          id2
        )(loc)
        
      predicatePullups(split, ctx) mustEqual expected
    }

    "pull a predicate out of a solve where both the filtered and the unfiltered set occur in the body" in {
      val rawInput = """
        | medals := //summer_games/london_medals
        | 
        | solve 'gender
        |   medals' := medals where medals.Gender = 'gender & medals.Edition = 2000
        |   { gender1: 'gender, gender2: medals.Gender, gender3: medals'.Gender }
        | """.stripMargin
        
      val id = new Identifier
      
      val loc = instructions.Line(1, 1, "")

      val load = LoadLocal(Const(CString("/summer_games/london_medals"))(loc))(loc) 
        
      val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              UnfixedSolution(1,
                Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("Gender"))(loc))(loc)
              ),
              Extra(
                Join(instructions.Eq,CrossLeftSort,
                  Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("Edition"))(loc))(loc),
                  Const(CLong(2000))(loc)
                )(loc)
              )
            )
          ),
          Join(instructions.JoinObject,CrossLeftSort,
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("gender1"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.JoinObject,IdentitySort,
              Join(instructions.WrapObject,CrossLeftSort,
                Const(CString("gender2"))(loc),
                Join(instructions.DerefObject,CrossLeftSort,
                  load,
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc),
              Join(instructions.WrapObject,CrossLeftSort,
                Const(CString("gender3"))(loc),
                Join(instructions.DerefObject,CrossLeftSort,
                  SplitGroup(0,load.identities, id)(loc),
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)
      
      val filteredLoad =
        Filter(IdentitySort,
          load,
          Join(instructions.Eq,CrossLeftSort,
            Join(instructions.DerefObject,CrossLeftSort, load, Const(CString("Edition"))(loc))(loc),
            Const(CLong(2000))(loc)
          )(loc)
        )(loc)
        
      val expected =
        Split(
          Group(0,
            filteredLoad,
            UnfixedSolution(1,
              Join(instructions.DerefObject,CrossLeftSort,
                filteredLoad,
                Const(CString("Gender"))(loc)
              )(loc)
            )
          ),
          Join(instructions.JoinObject,CrossLeftSort,
            Join(instructions.WrapObject,CrossLeftSort,
              Const(CString("gender1"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.JoinObject,IdentitySort,
              Join(instructions.WrapObject,CrossLeftSort,
                Const(CString("gender2"))(loc),
                Join(instructions.DerefObject,CrossLeftSort,
                  filteredLoad,
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc),
              Join(instructions.WrapObject,CrossLeftSort,
                Const(CString("gender3"))(loc),
                Join(instructions.DerefObject,CrossLeftSort,
                  SplitGroup(0, load.identities, id)(loc),
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)
        
      predicatePullups(split, ctx) mustEqual expected
    }
  }
}

object PredicatePullupSpecs extends PredicatePullupSpecs[YId] with yggdrasil.test.YIdInstances 
