import org.apache.spark.{SparkConf, SparkContext}

object IndustryGrowthAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IndustryGrowth_NetProfitOnly")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val filePath = "C:\\Users\\ssardana\\OneDrive - Capgemini\\Desktop\\OneDrive_1_23-6-2025 (1)\\Industry_Analysis_Src01.csv"
    val targetIndustry = "Information Technology Services"

    val raw = sc.textFile(filePath)
    val header = raw.first()
    val data = raw.filter(_ != header)

    // Extract: Industry, Company, Net Profit Margin
    val parsed = data.map(_.split(",")).map(fields => {
      val industry = fields(0).trim
      val company = fields(1).trim
      val margin = fields(10).toDouble
      ((industry, company), margin)
    })

    // Filter to the target industry
    val filtered = parsed.filter { case ((industry, _), _) => industry == targetIndustry }

    // Keep latest record per company (simple last-record assumption)
    val latestMargins = filtered.reduceByKey((_, latest) => latest)
    val companyMargins = latestMargins.map { case ((_, company), margin) => (company, margin) }

    // Normalize Net Profit Margins
    val marginValues = companyMargins.map(_._2)
    val minMargin = marginValues.min()
    val maxMargin = marginValues.max()

    val normalized = companyMargins.map { case (company, margin) =>
      val marginNorm = if (maxMargin != minMargin) (margin - minMargin) / (maxMargin - minMargin) else 0.0
      (company, marginNorm)
    }

    // Compute average industry growth score
    val totalScore = normalized.map(_._2).sum()
    val count = normalized.count()
    val avgGrowth = if (count > 0) totalScore / count else 0.0

    println(s"\n Growth Score for '$targetIndustry' (based only on Net Profit Margin): $avgGrowth%1.4f\n")
    println("Company-wise Contributions:")

    normalized.collect().sortBy(-_._2).foreach { case (company, score) =>
      println(f"$company%-40s → $score%1.4f")
    }

    sc.stop()
  }
}