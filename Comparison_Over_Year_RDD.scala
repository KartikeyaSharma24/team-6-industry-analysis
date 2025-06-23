import org.apache.spark.{SparkConf, SparkContext}

object Comparison_Over_Year_RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Company Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Industry_Analysis_Src01.csv")
    val header = data.first()

    data.cache()

    val result = data
      .filter(_ != header)
      .map(_.split(",", -1))
      .map(fields => {
        val company = fields(1).trim
        val year = fields(5).trim.takeRight(4)
        val marketCap = fields(7).toDouble
        val pe = fields(8).toDouble
        val roe = fields(9).toDouble
        val npm = fields(10).toDouble
        ((company, year), (marketCap, pe, roe, npm, 1))
      })
      .reduceByKey((a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5)
      )
      .mapValues { case (mc, pe, roe, npm, count) =>
        (mc / count, pe / count, roe / count, npm / count)
      }
      .sortBy({ case ((company, year), _) => (company, year) }) // Sort by company and year

    result.collect().foreach {
      case ((company, year), (avgMC, avgPE, avgROE, avgNPM)) =>
        println(s"$company | $year | MarketCap: $avgMC | P/E: $avgPE | ROE%: $avgROE | NetProfitMargin: $avgNPM")
    }

    result.saveAsTextFile("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Analyzed\\Comparison_Over_Year_RDD")


  }
}
