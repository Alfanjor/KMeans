package Kmeans

import Math.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext

object KmeansLearning extends Serializable {

  def learning(sc: SparkContext, optimalK: Int, percentile: Int): Unit = {

    def distance (a: org.apache.spark.mllib.linalg.Vector, b: org.apache.spark.mllib.linalg.Vector) = sqrt(Vectors.sqdist(a,b))

    sc.setJobDescription("Kmeans_Learning")
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val path = "hdfs://hvi1x0220:8020/user/biguardian/KMeans/"

//    val df = hiveContext.sql("SELECT src_ip, dst_ip, day, tsegm_inittime, week_day, " +
//      "n_events," +
//      "n_src_port," +
//      "n_dst_port," +
//      "n_tcp_protocol," +
//      "n_udp_protocol," +
//      "n_icmp_protocol," +
//      "n_unknown_protocol," +
//      "n_octetos," +
//      "n_octetos_reverse," +
//      "n_paquets," +
//      "n_paquets_reverse," +
//      "n_idle_timeout_endreason," +
//      "n_active_timeout_endreason," +
//      "n_end_of_flow_detected_endreason," +
//      "n_forced_end_endreason," +
//      "n_lack_of_resources_endreason," +
//      "n_exported," +
//      "n_dropped," +
//      "n_ignored," +
//      "n_tcp_urg_count " +
//      "FROM biguardian_prod.kmeans_connections WHERE day BETWEEN '2018-05-02' AND '2018-05-14'").coalesce(40)
//    val filledDf = df.na.fill(0)
    val filledDf = hiveContext.read.load(path + "KMeans_Training_Set")

    val dataRaw = filledDf.rdd.map{x => {
      (x.getString(0),   // src_ip
        x.getString(1),  // dst_ip
        x.getString(2),  // day
        x.getString(3),  // tsegm_initTime
        x.getString(4),  // week_day

        Vectors.dense(x.getInt(5), // n_events
          x.getInt(6),  // n_src_port
          x.getInt(7),  // n_dst_port
          x.getInt(8),  // n_tcp_protocol
          x.getInt(9),  // n_udp_protocol
          x.getInt(10), // n_icmp_protocl
          x.getInt(11), // n_unknown_protocol

          x.getInt(12), // n_octetos
          x.getInt(13), // n_octetos_reverse
          x.getInt(14), // n_paquets
          x.getInt(15), // n_paquets_reverse

          x.getInt(16), // n_idle_timeout_endReason
          x.getInt(17), // n_active_timeout_endReason
          x.getInt(18), // n_end_of_flow_detected_endReason
          x.getInt(19), // n_forced_end_endReason
          x.getInt(20), // n_lack_of_reseources_endReason

          x.getInt(21), // n_exported
          x.getInt(22), // n_dropped
          x.getInt(23), // n_ignored
          x.getInt(24)  // n_tcp_urg_count
        )
      )
    }}
    dataRaw.coalesce(40).persist()

    val features = dataRaw.map(x => x._6)
    features.persist()
    val scalerModel = new org.apache.spark.mllib.feature.StandardScaler(true, true).fit(features)

    // SAVE MEAN & STD OF DATASET
    val mean = scalerModel.mean
    val std = scalerModel.std

    val dataNormalized = dataRaw.map{ x =>
      (x._1,x._2,x._3,x._4,x._5,x._6,scalerModel.transform(x._6))
    }
    dataNormalized.coalesce(40)

    import hiveContext.implicits._

    val normalizedFeatures = dataNormalized.map(x => x._7)
    normalizedFeatures.persist()

//    val kmeansModel = org.apache.spark.mllib.clustering.KMeansModel.load(sc, path + "KMeans_Model_test")
    val kmeansModel = new org.apache.spark.mllib.clustering.KMeans().setSeed(1234).setK(optimalK).setMaxIterations(100).run(normalizedFeatures)
    val centers = kmeansModel.clusterCenters
    val predictions = dataNormalized.map{ x => {
      val prediction = kmeansModel.predict(x._7)
      val distanceToCenter = distance(x._7, centers(prediction))
      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,prediction, distanceToCenter)
    }}
    predictions.coalesce(40)

    val auxDF = predictions.toDF("src_ip","dst_ip","day","tsegm_inittime","week_day","features", "normalizedFeatures", "predictions", "distances")
    auxDF.coalesce(40)
//    val auxDF = hiveContext.read.load(path + "KMeans_Distances_Table_test")

    auxDF.registerTempTable("tempPredictionTable")
    val boundarys = hiveContext.sql("SELECT predictions, percentile_approx(distances, 0."+percentile.toString+") FROM tempPredictionTable GROUP BY predictions")

//    filledDf.write.save(path + "KMeans_Training_Set")
    sc.makeRDD(mean.toArray).coalesce(1).saveAsTextFile(path + "Mean")
    sc.makeRDD(std.toArray).coalesce(1).saveAsTextFile(path + "Std")
    kmeansModel.save(sc, path +"KMeans_Model_K"+optimalK+"_p"+percentile)
    sc.makeRDD(centers.toList).coalesce(1).saveAsTextFile(path + "KMeans_ClusterCenters")
    auxDF.write.save(path + "KMeans_Distances_Table")
    boundarys.coalesce(1).write.save(path + "KMeans_Boundarys_p"+percentile)

    sc.stop()
  }
}
