package com.precog.gjallerhorn

object RunAll extends Runner {
  def tasks(settings: Settings) = List(
    new AccountsTask(settings),
    new SecurityTask(settings),
    new MetadataTask(settings),
    new AnalyticsTask(settings),
    new IngestTask(settings),
    new ScenariosTask(settings)
  )
}
