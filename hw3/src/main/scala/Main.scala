import java.io._
import scala.util.Random

import breeze.linalg._
import breeze.stats._





object Main {

  def getWeights(x: DenseMatrix[Double], y: DenseVector[Double]): DenseVector[Double] = {
    val ALPHA: Int = 10
    val norm = DenseMatrix.eye[Double](x.cols).map(_ * ALPHA)
    val xSquare: DenseMatrix[Double] = x.t * x + norm
    val xSquareInverse: DenseMatrix[Double] = inv(xSquare)
    val weights: DenseVector[Double] = xSquareInverse * x.t * y
    weights
  }

  def crossValidate(
                     trainX: DenseMatrix[Double],
                     trainY: DenseVector[Double],
                     n: Int
                   ): Unit = {
    val splitSize: Int = trainX.rows / n
    val indexes: List[Int] = Random.shuffle(List.range(0, trainX.rows))
    val valid_metric = scala.collection.mutable.Map[Int, Double]()
    for (i <- 1 to n) {
      val lowerIndex = i * splitSize
      val upperIndex = (i + 1) * splitSize
      val testIndex = indexes.slice(lowerIndex, upperIndex - 1)
      val trainIndex = indexes.slice(0, lowerIndex - 1)
        .appendedAll(
          indexes.slice(upperIndex, trainX.rows)
        )
      val weights = getWeights(
        trainX(trainIndex, ::).toDenseMatrix,
        trainY(trainIndex).toDenseVector
      )
      val prediction = trainX(testIndex, ::).toDenseMatrix * weights
      val error = prediction - trainY(testIndex).toDenseVector
      val mse = error.t * error / trainX.rows
      valid_metric(i) = mse
      println(s"Iteration: $i MSE: $mse")
    }
    val file = "metrics.txt"
    val writer = new BufferedWriter(new FileWriter(file))
    for (element <- valid_metric){
      writer.write(element.toString())
      writer.newLine()
    }
    writer.flush()
    writer.close()
  }

  def normalize(trainX: DenseMatrix[Double], testX: DenseMatrix[Double]): Unit = {
    val stddevs = stddev(trainX, Axis._0).inner
    val means = mean(trainX, Axis._0).inner
    trainX(*,::) -= means
    testX(*,::) -= means
    trainX(*,::) /= stddevs
    testX(*,::) /= stddevs
  }

  def main(args: Array[String]): Unit = {
    val train = csvread(new File(args(0)),',')
    val test = csvread(new File(args(1)),',')
    val trainY = train(::, - 1).copy
    val trainX = train(::, 0 until train.cols - 1).copy
    normalize(trainX, test)
    val trainXNormed = DenseMatrix.horzcat(
      trainX,
      DenseMatrix.ones[Double](trainX.rows, 1)
    )
    val testXNormed = DenseMatrix.horzcat(
      test,
      DenseMatrix.ones[Double](test.rows, 1)
    )
    crossValidate(trainXNormed, trainY, 10)
    val weights: DenseVector[Double] = getWeights(trainXNormed, trainY)
    val predictions: DenseVector[Double] = testXNormed * weights
    val file = "predictions.txt"
    val writer = new BufferedWriter(new FileWriter(file))
    for (element <- predictions){
      writer.write(String.format("%.3f", element))
      writer.newLine()
    }
    writer.flush()
    writer.close()
  }
}
