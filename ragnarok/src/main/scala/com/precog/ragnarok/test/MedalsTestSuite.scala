//package com.precog
//package ragnarok
//package test
//
//object MedalsTestSuite extends PerfTestSuite {
//  query(
//    """
//      | import std::math::floor
//      | 
//      | historic := //summer_games/historic_medals
//      | 
//      | histogram := solve 'year
//      |   maleCount := count(historic.Gender where historic.Gender = "Men" & historic.Edition = 'year)
//      |   femaleCount := count(historic.Gender where historic.Gender = "Women" & historic.Edition = 'year)
//      | 
//      |   {year: 'year, ratio: floor(100 * maleCount / femaleCount)}
//      | 
//      | histogram
//      | """.stripMargin)
//}
