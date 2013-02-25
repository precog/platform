//package com.precog
//package ragnarok
//package test
//
//object BugTestSuite extends PerfTestSuite {
//  query(
//    """
//      | conversions := //conversions
//      |  
//      | result := solve 'customerId
//      |   conversions' := conversions where conversions.customer.ID = 'customerId
//      |  
//      |   {count: count(conversions'), customerId: 'customerId}
//      |  
//      | result
//      | """.stripMargin)
//}
