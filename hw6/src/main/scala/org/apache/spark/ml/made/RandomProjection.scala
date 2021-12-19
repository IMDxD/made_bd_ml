package org.apache.spark.ml.made

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.{LSH, LSHModel}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.ml.linalg
import org.apache.spark.sql.types.StructType

import scala.util.Random

class RandomProjection(override val uid: String) extends LSH[RandomProjectionModel] with HasSeed {

  def setSeed(value: Long): this.type = set(seed, value)

  override protected[this] def createRawLSHModel(inputDim: Int): RandomProjectionModel = {
    val rand = new Random($(seed))
    val randOnesVectors: Array[Vector] = {
      Array.fill($(numHashTables)) {
        val randArray = Array.fill(inputDim)(rand.nextInt(2) * 2 - 1)
        Vectors.fromBreeze(breeze.linalg.Vector(randArray))
      }
    }
    new RandomProjectionModel(uid, randOnesVectors)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    validateAndTransformSchema(schema)
  }

  def this() = this(Identifiable.randomUID("rp-lsh"))

}

class RandomProjectionModel(
                             override val uid: String,
                             private[ml] val randOnesVectors: Array[Vector]
                           ) extends LSHModel[RandomProjectionModel] {

  override protected[ml] def hashFunction(elems: linalg.Vector): Array[linalg.Vector] = {
    val hashValues = randOnesVectors.map(
      randOnesVector => {
        if (randOnesVector.asBreeze.dot(elems.asBreeze) > 0){
          1
        } else {
          0
        }
      }
    )
    hashValues.map(Vectors.dense(_))
  }

  override protected[ml] def keyDistance(x: linalg.Vector, y: linalg.Vector): Double = {
    val x_norm = breeze.linalg.normalize(x.asBreeze)
    val y_norm = breeze.linalg.normalize(y.asBreeze)
    val cos_xy = x_norm.dot(y_norm)
    Math.acos(cos_xy)
  }

  override protected[ml] def hashDistance(x: Seq[linalg.Vector], y: Seq[linalg.Vector]): Double = {
    x.zip(y).map(vectorPair => {
      val left = vectorPair._1
      val right = vectorPair._2
      val diff = breeze.numerics.abs(left.asBreeze - right.asBreeze)
      breeze.linalg.sum(diff)
    }).min
  }

  override def copy(extra: ParamMap): RandomProjectionModel = {
    val copied = new RandomProjectionModel(uid, randOnesVectors).setParent(parent)
    copyValues(copied, extra)
  }

  private[made] def this(randOnesVectors: Array[Vector]) =
    this(Identifiable.randomUID("rp-lshmodel"), randOnesVectors)

  override def write: MLWriter = {
    new RandomProjectionModel.RandomProjectionLModelWriter(this)
  }
}

object RandomProjectionModel {
  private[RandomProjectionModel] class RandomProjectionLModelWriter(
                                                                     instance: RandomProjectionModel) extends MLWriter {

    private case class Data(randUnitVectors: Matrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val numRows = instance.randOnesVectors.length
      require(numRows > 0)
      val numCols = instance.randOnesVectors.head.size
      val values = instance.randOnesVectors.map(_.toArray).reduce(Array.concat(_, _))
      val randMatrix = Matrices.dense(numRows, numCols, values)
      val data = Data(randMatrix)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }
}