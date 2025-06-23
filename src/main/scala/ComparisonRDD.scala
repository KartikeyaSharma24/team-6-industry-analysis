import org.apache.spark.{SparkConf, SparkContext}

object ComparisonRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Comparison_of_Different_Companies")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val  empRDD = sc.textFile("C:\\Users\\ksharm24\\OneDrive - Capgemini\\Industry Analysis\\Industry_Analysis_Src02.csv")

    val header = empRDD.first()
    val parsedEmpRDD = empRDD.filter(_ != header).map(_.split(",")).map(fields =>
      (fields(0), fields(1), fields(5).toDouble) // (Company_Name, Designation, Avg_Salary)
    )
      .cache()

    val targetDesignation = "System Engineer"

    val maxSalaryByCompany = parsedEmpRDD
      .filter(_._2 == targetDesignation)
      .map { case (company, _, salary) => (company, salary) }
      .reduceByKey((a, b) => math.max(a, b))
      .sortBy(_._2, ascending = false)

    maxSalaryByCompany.collect().foreach(println)

  }
}

