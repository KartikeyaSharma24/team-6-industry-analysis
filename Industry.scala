import org.apache.spark.{SparkConf, SparkContext}
object Industry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Ranking_of_companies_on_the_basis_of_different_growth_factor")
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
    val  rdd = sc.textFile("C:\\Users\\ssardana\\OneDrive - Capgemini\\Desktop\\OneDrive_1_23-6-2025 (1)\\Industry_Analysis_Src01.csv")
    rdd.take(5)

    rdd.collect().foreach(println)

  }}