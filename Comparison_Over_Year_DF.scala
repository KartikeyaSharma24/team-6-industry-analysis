import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Comparison_Over_Year_DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Company Analysis with DataFrame")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Industry_Analysis_Src01.csv")

    // Extract year from Date column (format: dd-MM-yyyy)
    val dfWithYear = df.withColumn("Year", year(to_date(col("Date"), "dd-MM-yyyy")))

    // Group by Company and Year, then calculate averages
    val avgDF = dfWithYear
      .groupBy("Company Name", "Year")
      .agg(
        avg("Market_Capital(in Million)").alias("Market_Cap"),
        avg("P/E").alias("P/E"),
        avg("ROE%").alias("Avg_ROE"),
        avg("Net_Profit_Margin").alias("NPM")
      )
      .orderBy("Company Name", "Year")

    // Show the result
    avgDF.show()


    avgDF.write.option("header", "true").csv("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Analyzed\\Comparison_Over_Year_DF")
  }
}






//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//
//object Comparison_Over_Year_DF {
//  case class CompanyMetrics(company: String, year: String, marketCap: Double, pe: Double, roe: Double, npm: Double)
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder
//      .appName("Company Analysis with DataFrame")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // Read the RDD output as plain text
//    val rawDF = spark.read.textFile("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Analyzed\\Comparison_Over_Year_RDD")
//
//    // Parse the text lines into structured case class
//    val parsedDF = rawDF.map(line => {
//      val cleaned = line.replace("(", "").replace(")", "")
//      val parts = cleaned.split(",", -1).map(_.trim)
//
//      val company = parts(0)
//      val year = parts(1)
//      val marketCap = parts(2).toDouble
//      val pe = parts(3).toDouble
//      val roe = parts(4).toDouble
//      val npm = parts(5).toDouble
//
//      CompanyMetrics(company, year, marketCap, pe, roe, npm)
//    }).toDF()
//
//    // Show parsed DataFrame
//    parsedDF.show()
//
//    // Order by company and year
//    val orderedDF = parsedDF.orderBy("company", "year")
//    orderedDF.show()
//
//    // Save as CSV
//    //orderedDF.write.option("header", "true").csv("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Analyzed\\Comparison_Over_Year_DF")
//  }
//}
