package Kmeans.KafkaStreaming

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ListBuffer

object KmeansOptimalK extends Serializable {

  def getElbowPoints (sc: SparkContext): Unit = {
    sc.setJobDescription("Obtain dataframe") //optimal K for KMeans
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val path = "hdfs://hvi1x0220:8020/user/biguardian/amedina/"

    //GET DATA TRAIN (A MONTH OF DATA?)
    val df = hiveContext.sql("SELECT src_ip, dst_ip, tsegm_inittime, week_day, day, " +
      "n_events," +
      "n_src_port," +
      "n_dst_port," +
      "n_tcp_protocol," +
      "n_udp_protocol," +
      "n_icmp_protocol," +
      "n_unknown_protocol," +
      "n_octetos," +
      "n_octetos_reverse," +
      "n_paquets," +
      "n_paquets_reverse," +
      "n_idle_timeout_endreason," +
      "n_active_timeout_endreason," +
      "n_end_of_flow_detected_endreason," +
      "n_forced_end_endreason," +
      "n_lack_of_resources_endreason," +
      "n_exported," +
      "n_dropped," +
      "n_ignored," +
      "n_tcp_urg_count " +
      "FROM biguardian_prod.kmeans_connections WHERE day BETWEEN '2018-04-20' AND '2018-05-20'").coalesce(40)
//      .write.save("hdfs://hvi1x0220:8020/user/extjaceiton/KMeans_Training_Set")
    val filledDf = df.na.fill(0)
    filledDf.write.save(path + "KMeans_Training_Set")

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
    }}.coalesce(40)
    val dataset = dataRaw.map(x => x._6)
    dataset.persist()

    // NORMALIZED DATASET
    val scalerModel = new org.apache.spark.mllib.feature.StandardScaler(true, true).fit(dataset)
    dataset.foreach(vector => scalerModel.transform(vector))

    // SAVE MEAN & STD OF DATASET
    val mean = scalerModel.mean
    val std = scalerModel.std
    sc.makeRDD(mean.toArray).saveAsTextFile("hdfs://hvi1x0220:8020/user/biguardian/amedina/MeanRDD")
    sc.makeRDD(std.toArray).saveAsTextFile("hdfs://hvi1x0220:8020/user/biguardian/amedina/StdRDD")
//    sc.parallelize(Seq(mean)).saveAsObjectFile("hdfs://hvi1x0220:8020/user/biguardian/amedina/MeanRDD")
//    sc.parallelize(Seq(std)).saveAsObjectFile("hdfs://hvi1x0220:8020/user/biguardian/amedina/StdRDD")

    // TRAIN KMEANS MODEL TO MAKE ELBOW MOTHOD AND GET OPTIMAL K
    val kmeansTraining = new org.apache.spark.mllib.clustering.KMeans().setSeed(1234)
    var elbowPoints = new ListBuffer[(Double, Double)]()

    for (i <- 2 to 60) {
      val kmeansModel = kmeansTraining.setK(i).run(dataset)
      val derp = (i.toDouble, kmeansModel.computeCost(dataset))
      elbowPoints += derp
    }

    // SAVE POINTS TO DRAW GRAPHIC OF ELBOW
    sc.parallelize(elbowPoints.toList).coalesce(1).saveAsTextFile("hdfs://hvi1x0220:8020/user/biguardian/amedina/ElbowPoints-20ABR_20MAY")
    sc.stop()
  }
}