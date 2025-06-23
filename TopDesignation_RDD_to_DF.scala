import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TopDesignation_RDD_to_DF {
  case class CompanyRecord(
                            CompanyName: String,
                            Designation: String,
                            ExperienceRange: String,
                            No_Of_Emp: String,
                            Date: String,
                            Avg_Salary: Double
                          )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("RDD to DataFrame Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read RDD
    val rawRDD = spark.sparkContext.textFile("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Industry_Analysis_Src02.csv")

    // Extract header
    val header = rawRDD.first()

    // Convert to case class RDD
    val companyRDD = rawRDD
      .filter(_ != header)
      .map(_.split(",", -1))
      .filter(_.length >= 6)
      .map(fields => CompanyRecord(
        fields(0),
        fields(1),
        fields(2),
        fields(3),
        fields(4),
        fields(5).toDouble
      ))

    // Convert to DataFrame
    val companyDF = companyRDD.toDF().cache()

    // Analysis: Top designation per company based on highest average salary
    val windowSpec = org.apache.spark.sql.expressions.Window.partitionBy("CompanyName").orderBy(desc("Avg_Salary"))

    val topDesignations = companyDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select("CompanyName", "Designation", "Avg_Salary")

    // Show result
    topDesignations.show()

    topDesignations.write.option("header", "true").csv("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Analyzed\\TopDesignation_RDD_to_DF")


  }
}
