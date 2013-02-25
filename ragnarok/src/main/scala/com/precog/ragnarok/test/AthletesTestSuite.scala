//package com.precog
//package ragnarok
//package test
//
//object AthletesTestSuite extends PerfTestSuite {
//  query(
//    """
//      | athletes := //summer_games/athletes
//      | 
//      | solve 'athlete
//      | athlete := athletes where athletes = 'athlete
//      |
//      | athlete' := {
//      |   "Countryname": athlete.Countryname,
//      |   "Population": athlete.Population,
//      |   "Sex": athlete.Sex,
//      |   "Sportname": athlete.Sportname,
//      |   "Name": athlete.Name
//      | }
//      |
//      | { count: count(athlete), athlete: athlete' }
//      | """.stripMargin)
//}
