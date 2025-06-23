import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RDDtoDFAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompanyRankingByEmployees_DF")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "C:\\Users\\ssardana\\OneDrive - Capgemini\\Desktop\\OneDrive_1_23-6-2025 (1)\\Industry_Analysis_Src02.csv"
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Repartition data by company for optimized grouping
    val partitionedDF = rawDF.repartition($"Company Name")

    // Aggregate by company
    val rankedDF = partitionedDF
      .filter($"No_Of_Emp".isNotNull)
      .withColumn("No_Of_Emp", $"No_Of_Emp".cast("int"))
      .groupBy($"Company Name")
      .agg(sum($"No_Of_Emp").as("Total_Employees"))
      .orderBy(desc("Total_Employees"))

    rankedDF.show(truncate = false)

    spark.stop()
  }
}
