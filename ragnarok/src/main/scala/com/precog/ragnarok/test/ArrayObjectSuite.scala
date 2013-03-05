package com.precog
package ragnarok
package test

object ArrayObjectSuite extends PerfTestSuite {
  "array joining" := {
    query("""
      medals' := //summer_games/london_medals
      medals'' := new medals'

      medals'' ~ medals'
      [medals', medals'', medals'] where medals'.Total = medals''.Total""")
  }

  "object joining" := {
    query("""
      medals' := //summer_games/london_medals
      medals'' := new medals'
      
      medals'' ~ medals'
      { a: medals', b: medals'', c: medals' } where medals'.Total = medals''.Total""")
  }
}
