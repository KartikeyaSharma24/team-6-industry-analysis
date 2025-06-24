import org.apache.spark.sql.{SparkSession, functions => F}

object SprintDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NetProfitMarginByYear")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Step 1: Load the CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/divyansg/Desktop/OneDrive_4_23-6-2025/Industry_Analysis_Src01.csv")

    // Step 2: Clean and extract Year
    val withYear = df
      .withColumn("Year", F.year(F.to_date($"Date", "dd-MM-yyyy")))
      .withColumn("Net_Profit_Margin", $"Net_Profit_Margin".cast("double"))
      .filter($"Year".between(2009, 2011) && $"Net_Profit_Margin".isNotNull)

    // Step 3: Compute average NPM per Company, per Year
    val yearlyAverages = withYear.groupBy($"Company Name", $"Year")
      .agg(F.avg($"Net_Profit_Margin").as("Avg_NPM"))

    // Step 4: Pivot to get years as columns
    val pivoted = yearlyAverages.groupBy($"Company Name")
      .pivot("Year", Seq(2009, 2010, 2011))
      .agg(F.round(F.avg("Avg_NPM"), 2))

    // Final result: just year-wise breakdown
    pivoted.orderBy($"Company Name").show(truncate = false)


  }
}