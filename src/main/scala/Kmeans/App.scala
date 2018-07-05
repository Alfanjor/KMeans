package Kmeans

import Kmeans.KafkaStreaming.KmeansOptimalK
import Kmeans.KafkaStreaming.model.KMeansStreamingDataSet
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kmeans_HistoricLoad")
    val sc = new SparkContext(conf)

//    KMeansStreamingDataSet.KmeansStreamingDataSet(sc)
//    KmeansOptimalK.getElbowPoints(sc)
//    KmeansLearning.learning(sc, args(0).toInt, args(1).toInt)
//    KMeansPrediction.KmeansPredictionStreaming(sc)

    // EXTRAS
    HistoryLoad.load(sc)
//    KmeansAnomalyTest.load(sc, args(0).toInt, args(1).toInt)
  }
}
