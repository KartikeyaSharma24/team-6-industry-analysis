import org.apache.spark.{SparkConf, SparkContext}

object Sprint {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Comparison_of_Different_Companies")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // STEP 1: Load file
    val filePath = "C:\\Users\\divyansg\\Desktop\\OneDrive_4_23-6-2025\\Industry_Analysis_Src01.csv"
    val raw = sc.textFile(filePath)

    // STEP 2: Remove header
    val header = raw.first()
    val data = raw.filter(_ != header)

    // STEP 3: Parse relevant columns as (Company, Year, Net_Profit_Margin)
    val parsed = data.map(line => {
      val cols = line.split(",")
      val company = cols(1).trim
      val dateStr = cols(5).trim
      val year = {
        val y = dateStr.split("-")(2).trim
        if (y.length == 2) ("20" + y).toInt else y.toInt
      }
      val margin = cols(10).trim.toDouble
      (company, year, margin)
    })

    val cached = parsed.cache()

    // STEP 4: Identify latest 3 years in data
    val top3Years = cached.map(_._2).distinct().sortBy(x => -x).take(3).toSet

    // STEP 5: Filter only for those 3 years
    val recent = cached.filter { case (_, year, _) => top3Years.contains(year) }.cache()

    // STEP 6: Get average Net Profit Margin per (Company, Year)
    val avgByYear = recent
      .map { case (company, year, margin) => ((company, year), (margin, 1)) }
      .reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) => (sum1 + sum2, cnt1 + cnt2) }
      .mapValues { case (sum, count) => sum / count }

    // STEP 7: Group by Company
    val grouped = avgByYear
      .map { case ((company, year), avgMargin) => (company, (year, avgMargin)) }
      .groupByKey()
      .mapValues(_.toList.sortBy(_._1))

    // STEP 8: Compute year-over-year growth
    val growth = grouped.mapValues(values =>
      values.sliding(2).collect {
        case List((y1, m1), (y2, m2)) if m1 != 0 =>
          (y1, y2, ((m2 - m1) / m1) * 100)
      }.toList
    )

    // STEP 9: Print results
    println("Company Net Profit Margin Growth (last 3 years):")
    growth.collect().foreach { case (company, records) =>
      println(s"\n$company")
      records.foreach { case (from, to, change) =>
        println(f"  From $from to $to: $change%.2f%%")
      }
    }

    sc.stop()
  }
}