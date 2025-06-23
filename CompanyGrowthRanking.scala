import org.apache.spark.{SparkConf, SparkContext}

object CompanyGrowthRanking {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("CompanyRankingByEmployees")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val filePath = "C:\\Users\\ssardana\\OneDrive - Capgemini\\Desktop\\OneDrive_1_23-6-2025 (1)\\Industry_Analysis_Src02.csv"
    val raw = sc.textFile(filePath)
    val header = raw.first()
    val data = raw.filter(_ != header)

    // Extract: (Company Name, No_Of_Emp)
    val parsed = data
      .map(_.split(",", -1))
      .filter(_.length >= 4)
      .map(fields => {
        val company = fields(0).trim
        val empCount = fields(3).trim.toInt
        (company, empCount)
      })
      .cache() // Caching parsed RDD

    // Aggregate total employees per company
    val aggregated = parsed
      .reduceByKey(_ + _)
      .sortBy(-_._2)

    println("\n Company Ranking by Total Number of Employees:\n")
    aggregated.collect().foreach { case (company, totalEmp) =>
      println(f"$company%-40s → Employees: $totalEmp")
    }

    sc.stop()
  }
}