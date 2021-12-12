import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("made-tfidf")
      .getOrCreate()
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(args(0))
    val dfWithReviewArray = df
      .withColumn(
        "ReviewArray",
        split(
          trim(
            regexp_replace(
              lower(col("Review")),
              "\\W+",
              " "
            )
          ),
          " "
        )
      )
    val dfWithReviewArrayRows = dfWithReviewArray.withColumn("row", monotonically_increasing_id())
    val dfExploded = dfWithReviewArrayRows
      .select(col("row"), explode(col("ReviewArray")).as("Word"))
      .groupBy("row", "Word")
      .agg(count("*").as("WordCount"))
      .select(col("row"), col("Word"), col("WordCount"))
    val dfWithDocCount = dfExploded
      .groupBy("Word")
      .agg(countDistinct(col("row")).as("WordDocuments"))
      .withColumn("DocumentCount", lit(df.count()))
      .orderBy(desc("WordDocuments")).limit(100)
    val dfDocLen = dfExploded
      .groupBy("row")
      .agg(sum(col("WordCount")).as("DocLen"))
    val dfWordCountDocLen = dfDocLen.as("left")
      .join(dfExploded.as("right"), dfDocLen("row") === dfExploded("row"))
      .select(
        col("left.row"),
        col("left.DocLen"),
        col("right.Word"),
        col("right.WordCount")
      )
    val dfTf = dfWordCountDocLen.withColumn("Tf", col("WordCount") / col("DocLen"))
    val dfIdf = dfWithDocCount
      .withColumn("Idf", log(
        col("DocumentCount") / col("WordDocuments")
      ))
      .select(col("Word"), col("Idf"))
    val dfJoined = dfTf.as("left")
      .join(dfIdf.as("right"), dfExploded("Word") === dfIdf("Word"))
      .select(
        col("row"),
        col("left.Word"),
        col("left.Tf"),
        col("right.Idf").as("Idf")
      )
    val dfTfIdf = dfJoined.withColumn("TfIdf", col("Tf") * col("Idf"))
    val result = dfTfIdf
      .groupBy("row")
      .pivot("Word")
      .agg(first(col("TfIdf")))
      .na.fill(0)
      .orderBy(col("row"))
      .drop(col("row"))
    result.repartition(1).write.option("header", "true").csv("spark_output/data")
  }
}
