import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object RDDtoDFSol {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Comparison_of_Different_Companies")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    val empRDD = sc.textFile("C:\\Users\\ksharm24\\OneDrive - Capgemini\\Industry Analysis\\Industry_Analysis_Src02.csv")

    val header = empRDD.first()

    val parsedEmpRDD = empRDD
      .filter(_ != header)
      .map(_.split(","))
      .map(fields => (fields(0), fields(1), fields(5).toDouble)) // (Company_Name, Designation, Avg_Salary)
      .cache()

    // Convert to DataFrame
    val empDF = parsedEmpRDD.toDF("Company_Name", "Designation", "Avg_Salary")

    // Define target designation
    val targetDesignation = "System Engineer"

    // Repartition by Company_Name before aggregation for better parallelism
    val partitionedDF = empDF.repartition($"Company_Name")

    val maxSalaryByCompany = partitionedDF
      .filter($"Designation" === targetDesignation)
      .groupBy("Company_Name")
      .agg(max("Avg_Salary").as("Max_Salary"))
      .orderBy(desc("Max_Salary"))

    maxSalaryByCompany.show()

    spark.stop()
  }
}