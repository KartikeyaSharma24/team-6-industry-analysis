import org.apache.spark.{SparkConf, SparkContext}
object Industry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Ranking_of_companies_on_the_basis_of_different_growth_factor")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val  rdd = sc.textFile("C:\\Users\\ssardana\\OneDrive - Capgemini\\Desktop\\OneDrive_1_23-6-2025 (1)\\Industry_Analysis_Src01.csv")
    rdd.take(5)

    rdd.collect().foreach(println)

  }}
