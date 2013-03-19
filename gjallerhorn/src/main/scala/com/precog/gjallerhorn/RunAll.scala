package com.precog.gjallerhorn

object RunAll extends Runner {
  def tasks(settings: Settings) = List(
    new AccountsTask(settings),
    new SecurityTask(settings),
    new MetadataTask(settings),
    new ScenariosTask(settings)
  )
}
