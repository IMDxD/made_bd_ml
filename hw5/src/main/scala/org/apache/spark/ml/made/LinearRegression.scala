package org.apache.spark.ml.made

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.apache.spark.sql.functions.lit


trait LinearRegressionParams extends HasFeaturesCol with HasLabelCol with HasOutputCol {
  def setFeatureCol(value: String) : this.type = set(featuresCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  val nIter = new IntParam(
    this, "nIter","number of iterations for gradient descent"
  )
  val lr = new DoubleParam(
    this, "lr","learning rate for gradient descent")
  def getNIter : Int = $(nIter)
  def setNIter(value: Int) : this.type = set(nIter, value)
  def getLr : Double = $(lr)
  def setLr(value: Double) : this.type = set(lr, value)

  setDefault(nIter -> 500)
  setDefault(lr -> 0.01)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, getFeaturesCol, new VectorUDT())
    SchemaUtils.checkColumnType(schema, getLabelCol, DoubleType)

    if (schema.fieldNames.contains($(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, DoubleType)
      schema
    } else {
      SchemaUtils.appendColumn(schema, StructField(getOutputCol, DoubleType))
    }
  }
}

class LinearRegression(override val uid: String) extends Estimator[LinearRegressionModel] with LinearRegressionParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("LinearRegression"))

  override def fit(dataset: Dataset[_]): LinearRegressionModel = {

    // Used to convert untyped dataframes to datasets with vectors
    implicit val encoder : Encoder[Vector] = ExpressionEncoder()

    val data = dataset.select(
      lit(1.0).as("Intercept"),
      dataset($(featuresCol)),
      dataset($(labelCol))
    )

    val assembler = new VectorAssembler()
      .setInputCols(Array("Intercept", $(featuresCol), $(labelCol)))
      .setOutputCol("concat")

    val assembledData = assembler.transform(data)
    val vectors: Dataset[Vector] = assembledData.select(assembledData("concat").as[Vector])

    val dim: Int = AttributeGroup.fromStructField(assembledData.schema("concat")).numAttributes.getOrElse(
      vectors.first().size
    )

    var weightsWithIntercept = Vectors.zeros(dim - 1).asBreeze.toDenseVector

    for (_ <- 1 to getNIter) {
      val summary = vectors.rdd.mapPartitions((data: Iterator[Vector]) => {
        val result = data.foldLeft(new MultivariateOnlineSummarizer())(
          (summarizer, vector) => summarizer.add(
            {
              val x = vector.asBreeze(0 until dim - 1).toDenseVector
              val y = vector.asBreeze(dim - 1)
              val diff = y - (x dot weightsWithIntercept)
              mllib.linalg.Vectors.fromBreeze(-2.0 * x * diff)
            }
          ))
        Iterator(result)
      }).reduce(_ merge _)

      weightsWithIntercept = weightsWithIntercept - summary.mean.asML.asBreeze.toDenseVector * $(lr)
    }

    val weights = Vectors.fromBreeze(weightsWithIntercept(1 until weightsWithIntercept.size))
    val intercept = weightsWithIntercept(0)

    copyValues(new LinearRegressionModel(
        weights,
        intercept
      )
    ).setParent(this)

  }

  override def copy(extra: ParamMap): Estimator[LinearRegressionModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

}

object LinearRegression extends DefaultParamsReadable[LinearRegression]

class LinearRegressionModel private[made](
                                           override val uid: String,
                                           val weights: DenseVector,
                                           val intercept: Double
                                         ) extends Model[LinearRegressionModel] with LinearRegressionParams with MLWritable {


  private[made] def this(weights: Vector, intercept: Double) =
    this(Identifiable.randomUID("LinearRegressionModel"), weights.toDense, intercept)

  override def copy(extra: ParamMap): LinearRegressionModel = copyValues(
    new LinearRegressionModel(weights, intercept), extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bWeights = weights.asBreeze
    val transformUdf = dataset.sqlContext.udf.register(uid + "_transform",
        (x : Vector) => {
          x.asBreeze.dot(bWeights) + intercept
        })

    dataset.withColumn($(outputCol), transformUdf(dataset($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new DefaultParamsWriter(this) {
    override protected def saveImpl(path: String): Unit = {
      super.saveImpl(path)
      val modelParams = weights.toArray :+ intercept
      sqlContext.createDataFrame(Seq(Tuple1(Vectors.dense(modelParams)))).write.parquet(path + "/vectors")
    }
  }
}

object LinearRegressionModel extends MLReadable[LinearRegressionModel] {
  override def read: MLReader[LinearRegressionModel] = new MLReader[LinearRegressionModel] {
    override def load(path: String): LinearRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc)

      val vectors = sqlContext.read.parquet(path + "/vectors")

      // Used to convert untyped dataframes to datasets with vectors
      implicit val encoder : Encoder[Vector] = ExpressionEncoder()
      val paramsTuple = vectors.select(vectors("_1")
        .as[Vector]).first().asBreeze.toDenseVector

      val weights = Vectors.fromBreeze(paramsTuple(0 until paramsTuple.size - 1))
      val intercept = paramsTuple(paramsTuple.size - 1)

      val model = new LinearRegressionModel(weights, intercept)
      metadata.getAndSetParams(model)
      model
    }
  }
}