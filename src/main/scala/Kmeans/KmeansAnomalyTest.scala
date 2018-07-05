package Kmeans

import java.lang.Math.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext

object KmeansAnomalyTest {

  def load(sc: SparkContext, optimalK: Int, percentile: Int): Unit = {

    def distance (a: org.apache.spark.mllib.linalg.Vector, b: org.apache.spark.mllib.linalg.Vector) = sqrt(Vectors.sqdist(a,b))

    sc.setJobDescription("Kmeans_Anomaly_test")
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val path = "hdfs://hvi1x0220:8020/user/extjaceiton/KMeans/Prueba_k"+optimalK+"_p"+percentile+"/"

    val csv = sc.textFile("hdfs://hvi1x0220:8020/user/extjaceiton/CSVs/testWithAnomalies.csv")
    val rows = csv.map(line => line.split(",").map(_.trim))
    val header = rows.first
    val data = rows.filter(_(0) != header(0))

    val dataRaw = data.map{ x =>
      (
        x(0), 	//src_ip
        x(1), 	//dst_ip
        x(2), 	//tsegm_initTime
        x(3), 	//week_day
        x(4), 	//day

        Vectors.dense(
          x(5).replaceAll("\"", "").toDouble, 	//n_events
          x(6).replaceAll("\"", "").toDouble, 	//n_src_port
          x(7).replaceAll("\"", "").toDouble, 	//n_dst_port
          x(8).replaceAll("\"", "").toDouble, 	//n_tcp_protocol
          x(9).replaceAll("\"", "").toDouble, 	//n_udp_protocol
          x(10).replaceAll("\"", "").toDouble,  //n_icmp_protocl
          x(11).replaceAll("\"", "").toDouble, 	//n_unknown_protocol

          x(12).replaceAll("\"", "").toDouble, 	//n_octetos
          x(13).replaceAll("\"", "").toDouble, 	//n_octetos_reverse
          x(14).replaceAll("\"", "").toDouble, 	//n_paquets
          x(15).replaceAll("\"", "").toDouble, 	//n_paquets_reverse

          x(16).replaceAll("\"", "").toDouble, 	//n_idle_timeout_endReason
          x(17).replaceAll("\"", "").toDouble, 	//n_active_timeout_endReason
          x(18).replaceAll("\"", "").toDouble, 	//n_end_of_flow_detected_endReason
          x(19).replaceAll("\"", "").toDouble, 	//n_forced_end_endReason
          x(20).replaceAll("\"", "").toDouble, 	//n_lack_of_reseources_endReason

          x(21).replaceAll("\"", "").toDouble, 	//n_exported
          x(22).replaceAll("\"", "").toDouble, 	//n_dropped
          x(23).replaceAll("\"", "").toDouble, 	//n_ignored
          x(24).replaceAll("\"", "").toDouble 	//n_tcp_urg_count
        ),

        x(25),  //Anomaly
        x(26),  //AnomalyType
        x(27)   //AnomalousVar
      )
    }

    var aux = sc.textFile(path + "Mean/part-00000")
    val mean = aux.collect()
    aux = sc.textFile(path + "Std/part-00000")
    val std = aux.collect()

    // NORMALIZAR TABLA
    val Z = Seq(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0).to[scala.Vector].toArray
    val dataNormalized = dataRaw.map{ x => {
      for (i <- 0 to Z.size-1) {
        if (std(i).toDouble == 0.0)
          Z(i) = 0.0
        else
          Z(i) = ((x._6(i) - mean(i).toDouble) / std(i).toDouble)
      }
      val normalizedVector = Vectors.dense(Z)
      (x._1,x._2,x._3,x._4,x._5,x._6,normalizedVector,x._7,x._8,x._9)
    }}

    val kmeansModel = org.apache.spark.mllib.clustering.KMeansModel.load(sc, path + "KMeans_Model_K"+optimalK+"_p"+percentile)
    val centers = kmeansModel.clusterCenters

    val distancesPrediction = dataNormalized.map{ x => {
      val prediction = kmeansModel.predict(x._7)
      val distanceToCenter = distance(x._7, centers(prediction))
      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,prediction,distanceToCenter,x._8,x._9,x._10)
    }}

    aux = hiveContext.read.load(path + "KMeans_Boundarys").map(x => x(1).toString)
    val boundarysList = aux.collect()

    val muestra = distancesPrediction.map{x =>
      var anomaly = false
      if (x._9 > boundarysList(x._8).toDouble) {
        anomaly = true
      }
      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,boundarysList(x._8).toDouble,anomaly,x._10,x._11,x._12)
    }

    import hiveContext.implicits._
    val test = muestra.toDF("src_ip","dst_ip","tsegm_inittime","week_day","day",
      "features","normalizedFeatures","predictions","distances","bondary","pred_anom",
      "anomaly","anomaly_type","anomalous_var")
    test.coalesce(1).write.save(path + "testWithAnomalies")

    sc.stop()
  }

}
