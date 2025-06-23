import org.apache.spark.{SparkConf, SparkContext}

object TopDesignationRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Comparison_of_Different_Companies")
      .setMaster("local[*]")
//      .set("spark.executor.memory", "8g")
//      .set("spark.executor.cores", "4")
//      .set("spark.executor.instances", "10")
//      .set("spark.executor.memoryOverhead", "1g")
//      .set("spark.driver.memory", "4g")
//      .set("spark.driver.cores", "2")
//      .set("spark.default.parallelism", "100")
//      .set("spark.task.maxFailures", "4")
//    //      .set("spark.eventLog.enabled", "true")
//    //      .set("spark.eventLog.dir", "hdfs://namenode:9001/logs")
//    //      .set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")

    val sc = new SparkContext(conf)

    val  CompanyDf1= sc.textFile("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Industry_Analysis_Src02.csv")

    // Extract header
    val header = CompanyDf1.first()
    CompanyDf1.cache()
    // Remove header and split each line
    val dataRDD = CompanyDf1
      .filter(_ != header)
      .map(line => line.split(",", -1))

    // Map to ((Company, Designation), Avg_Salary)
    val companyDesignationSalary = dataRDD.map(fields => {
      val company = fields(0)
      val designation = fields(1)
      val salary = fields(5).toDouble
      ((company, designation), salary)
    })

    // Get max salary per (Company, Designation)
    val maxSalaryPerDesignation = companyDesignationSalary
      .reduceByKey((a, b) => math.max(a, b))

    // Group by company and find the designation with highest salary
    val topDesignationPerCompany = maxSalaryPerDesignation
      .map { case ((company, designation), salary) => (company, (designation, salary)) }
      .reduceByKey((a, b) => if (a._2 > b._2) a else b)

    // Display results
    topDesignationPerCompany.collect().foreach {
      case (company, (designation, salary)) =>
        println(s"Company: $company | Top Designation: $designation | Salary: ₹${salary}L")
    }

    topDesignationPerCompany.saveAsTextFile("C:\\Users\\kjindal\\OneDrive - Capgemini\\Desktop\\Industry Analysis\\Analyzed\\TopDesignationRDD")

    //    CompanyDf1.take(5)
//    CompanyDf1.collect().foreach(println)



  }}