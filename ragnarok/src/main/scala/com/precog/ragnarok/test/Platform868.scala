package com.precog
package ragnarok
package test

object Platform868 extends PerfTestSuite {
  query(
    """
billing     := //billing
conversions := //conversions

solve 'customerId
  billing'     := billing where billing.customer.ID = 'customerId
  conversions' := conversions where conversions.customer.ID = 'customerId

  billing' ~ conversions'
  {customerId: 'customerId, lastDate: billing'.date}
    """)
}
