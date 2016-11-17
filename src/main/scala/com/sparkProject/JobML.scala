package com.sparkProject

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

/**
  * Created by axel on 10/25/16.
  */
object JobML {

    def main(args: Array[String]): Unit = {

        // SparkSession configuration
        val spark = SparkSession.builder
            .appName("spark session TP_parisTech")
            .getOrCreate()

        // Read the csv from the first sessions
        var df = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("comment", "#")
            .csv("cleanedDataframe.csv")

        // Gather all the input columns in a columns of vectors, using vector assembler
        df = new VectorAssembler()
            .setInputCols(df.drop("rowid", "koi_disposition").columns)
            .setOutputCol("features")
            .transform(df)
            .select("koi_disposition", "features")

        // Instead of using string labels, create a indexed column
        df = new StringIndexer()
            .setInputCol("koi_disposition")
            .setOutputCol("label")
            .fit(df).transform(df)
            .select("label", "features")

        // Create a list that contains then values of a that we want to test
        var as:List[Double] = List[Double]()
        var a:Double = 1.0e-6
        val fact = math.sqrt(10)
        while (a <= 1.0e-2) {
            as = as :+ a
            a = a * fact
        }

        // Initialize a model (LogisticRegression) to use for fitting
        val model = new LogisticRegression()
            .setElasticNetParam(1.0)
            .setStandardization(true)
            .setFitIntercept(true)
            .setTol(1.0e-3)
            .setMaxIter(500)

        // Fit the model several times with different parameters and
        // pick the best model according to cross validation
        val paramsGrid = new ParamGridBuilder().addGrid(model.regParam, as).build()
        val Array(trainingDf, testDf) = df.randomSplit(Array(0.9, 0.1))
        val bestModel = new TrainValidationSplit()
            .setEstimator(model)
            .setEvaluator(new BinaryClassificationEvaluator())
            .setEstimatorParamMaps(paramsGrid)
            .setTrainRatio(0.7)
            .fit(trainingDf)

        // Apply the fit model on the test partition
        val predictions = bestModel.transform(testDf)

        // Evaluate and print the result of the fit model on the test dataset
        println("Best params: " + bestModel.bestModel.extractParamMap())
        println("Accuracy: " + bestModel.getEvaluator.evaluate(predictions))
    }
}