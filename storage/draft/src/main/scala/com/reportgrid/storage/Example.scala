package com.reportgrid.storage

object Example {
  def main(args: Array[String]) = {
    val salary = dataset[Long, Long]("/employees", ".salary")
    val years  = dataset[Long, Long]("/employees", ".years")

    val salaryAndYears = salary.join(years)

    val totalCost = salaryAndYears.map { tuple =>
      val salary = tuple._1
      val years  = tuple._2

      salary * years
    }.filter(_ > 200)
  }
}