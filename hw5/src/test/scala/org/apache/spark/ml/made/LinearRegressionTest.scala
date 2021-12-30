package org.apache.spark.ml.made

import scala.collection.JavaConverters._
import com.google.common.io.Files
import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.ml
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}

class LinearRegressionTest extends AnyFlatSpec with should.Matchers with WithSpark {

  val delta = 0.1
  lazy val data: DataFrame = LinearRegressionTest._data

  "Model" should "make prediction" in {
    val model: LinearRegressionModel = new LinearRegressionModel(
      weights = Vectors.dense(2.0, 3.0).toDense,
      intercept = 1.0
    ).setFeatureCol("features")
      .setLabelCol("label")
      .setOutputCol("predictions")

    val predictions = model.transform(data).collect().map(_.getAs[Double](2))

    predictions.length should be(3)

    validateModel(model, model.transform(data))
  }


  "Estimator" should "calculate weights" in {
    val estimator = new LinearRegression()
      .setFeatureCol("features")
      .setLabelCol("label")
      .setOutputCol("predictions")

    estimator.setLr(0.01)
    estimator.setNIter(1000)

    val model = estimator.fit(data)

    model.weights(0) should be(2.0 +- delta)
    model.weights(1) should be(3.0 +- delta)
  }

  "Estimator" should "not fit with zero lr" in {
    val estimator = new LinearRegression()
      .setFeatureCol("features")
      .setLabelCol("label")
      .setOutputCol("predictions")

    estimator.setLr(0.0)

    val model = estimator.fit(data)

    model.weights(0) should be(0.0 +- 1e-4)
    model.weights(1) should be(0.0 +- 1e-4)
    model.intercept should be(0.0 +- 1e-4)
  }

  "Estimator" should "not fit with zero nIter" in {
    val estimator = new LinearRegression()
      .setFeatureCol("features")
      .setLabelCol("label")
      .setOutputCol("predictions")

    estimator.setNIter(0)

    val model = estimator.fit(data)

    model.weights(0) should be(0.0 +- 1e-4)
    model.weights(1) should be(0.0 +- 1e-4)
    model.intercept should be(0.0 +- 1e-4)
  }

  "Estimator" should "calculate intercept" in {
    val estimator = new LinearRegression()
      .setFeatureCol("features")
      .setLabelCol("label")
      .setOutputCol("predictions")

    val model = estimator.fit(data)

    model.intercept should be(1.0 +- delta)
  }

  "Estimator" should "should produce functional model" in {
    val estimator = new LinearRegression()
      .setFeatureCol("features")
      .setLabelCol("label")
      .setOutputCol("predictions")

    val model = estimator.fit(data)

    validateModel(model, model.transform(data))
  }

  private def validateModel(model: LinearRegressionModel, data: DataFrame) = {
    val predictions: Array[Double] = data.collect().map(_.getAs[Double](2))

    predictions.length should be(3)

    predictions(0) should be(1.0 + 1.0 * 2.0 + 2.0 * 3.0 +- delta)
    predictions(1) should be(1.0 + 4.0 * 2.0 + 3.0 * 3.0 +- delta)
    predictions(2) should be(1.0 - 1.0 * 2.0 + 3.0 * 3.0 +- delta)

  }

  "Estimator" should "work after re-read" in {

    val pipeline = new Pipeline().setStages(Array(
      new LinearRegression()
        .setFeatureCol("features")
        .setLabelCol("label")
        .setOutputCol("predictions")
    ))

    val tmpFolder = Files.createTempDir()

    pipeline.write.overwrite().save(tmpFolder.getAbsolutePath)

    val reRead = Pipeline.load(tmpFolder.getAbsolutePath)

    val model = reRead.fit(data).stages(0).asInstanceOf[LinearRegressionModel]

    model.weights(0) should be(2.0 +- delta)
    model.weights(1) should be(3.0 +- delta)
    model.intercept should be(1.0 +- delta)

    validateModel(model, model.transform(data))
  }

  "Model" should "work after re-read" in {

    val pipeline = new Pipeline().setStages(Array(
      new LinearRegression()
        .setFeatureCol("features")
        .setLabelCol("label")
        .setOutputCol("predictions")
    ))

    val model = pipeline.fit(data)

    val tmpFolder = Files.createTempDir()

    model.write.overwrite().save(tmpFolder.getAbsolutePath)

    val reRead: PipelineModel = PipelineModel.load(tmpFolder.getAbsolutePath)

    validateModel(model.stages(0).asInstanceOf[LinearRegressionModel], reRead.transform(data))
  }
}

object LinearRegressionTest extends WithSpark {

  lazy val schema: StructType = StructType(
    Array(
      StructField("label", DoubleType),
      StructField("features", new VectorUDT())
  ))

  lazy val rowData = List(
    Row(9.0, Vectors.dense(1.0, 2.0)),
    Row(18.0, Vectors.dense(4.0, 3.0)),
    Row(8.0, Vectors.dense(-1.0, 3.0))
  )

  lazy val _data: DataFrame = sqlc.createDataFrame(rowData.asJava, schema)

}
