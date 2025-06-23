import org.apache.spark.{SparkConf, SparkContext}

object IndustryDA {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Comparison_of_Different_Companies")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val marketRDD = sc.textFile("C:\\Users\\ksharm24\\OneDrive - Capgemini\\Industry Analysis\\Industry_Analysis_Src01.csv")
    val marketHeader = marketRDD.first()

    val parsedMarketRDD = marketRDD.filter(_ != marketHeader).map(_.split(",")).map(fields =>
      (fields(2), fields(0), fields(5).split("-")(2), fields(7).toDouble) // (Country, Industry, Year, Market_Capital)
    )

    val groupedByCountryIndustryYear = parsedMarketRDD
      .map { case (country, industry, year, marketCap) =>
        ((country, industry, year), marketCap)
      }
      .reduceByKey(_ + _)
      .cache()

//    groupedByCountryIndustryYear.collect().foreach(println)
    groupedByCountryIndustryYear
      .sortByKey()
      .collect()
      .foreach(println)
  }
}