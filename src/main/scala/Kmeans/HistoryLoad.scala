package Kmeans

import java.lang.Math.sqrt

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext

object HistoryLoad {

  def load(sc: SparkContext): Unit = {

    def distance (a: org.apache.spark.mllib.linalg.Vector, b: org.apache.spark.mllib.linalg.Vector) = sqrt(Vectors.sqdist(a,b))

    sc.setJobDescription("Kmeans_HistoricLoad")
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val path = "hdfs://hvi1x0220:8020/user/biguardian/KMeans/"

    val df = hiveContext.sql("SELECT src_ip, dst_ip, day, tsegm_inittime, week_day, " +
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
      "FROM biguardian_prod.kmeans_connections WHERE day BETWEEN '2018-05-01' AND '2018-05-06'").coalesce(40)
    val filledDf = df.na.fill(0)

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
      (x._1,x._2,x._3,x._4,x._5,x._6, normalizedVector)
    }}

    val kmeansModel = org.apache.spark.mllib.clustering.KMeansModel.load(sc, path + "KMeans_Model_K11_p92")
    val centers = kmeansModel.clusterCenters

    val distancesPrediction = dataNormalized.map{ x => {
      val prediction = kmeansModel.predict(x._7)
      val distanceToCenter = distance(x._7, centers(prediction))
      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,prediction,distanceToCenter)
    }}

    aux = hiveContext.read.load(path + "KMeans_Boundarys_p92").map(x => x(1).toString)
    val boundarysList = aux.collect()

    val anomalies = distancesPrediction.filter(row => (row._9 > boundarysList(row._8).toDouble)).map{ x => {

      val post = new HttpPost("http://hvi1x0194:8983/solr/biguardian_anomaly_prod/update")
      post.setHeader("Content-type", "application/json")

      val rand = new scala.util.Random(System.currentTimeMillis())
      val event_type = "KMEANS_BASE"
      val id = "Oesia-1-d258a938-44b1-11e7-b6a9-0cc"+rand.nextInt(100)+"ae"+rand.nextInt(100)+"c"+rand.nextInt(100)
      val timestamp = x._3+" "+x._4
      val src_port = rand.nextInt(91234)
      val dst_port = rand.nextInt(91234)
      val protocol = rand.nextInt(25)
      val probability = 1 - (boundarysList(x._8).toDouble/x._9)

      val commandLine = "[{?id?:?"+id+"?, ?anomaly_type?:?"+event_type+"?, ?protocol?:?"+protocol+"?, ?features?:?"+x._6.toString+
        "?, ?probability?:?"+probability+"?, ?timestamp?:?"+timestamp+"?, ?src_port?:?"+src_port+"?, ?prediction?:?"+x._8+
        "?, ?src_ip?:?"+x._1+"?, ?dst_port?:?"+dst_port+"?, ?dst_ip?:?"+x._2+"?, ?day?:?"+x._3+"?}]"

      post.setEntity(new StringEntity(commandLine.replace("?","\"")))
      val response = (new DefaultHttpClient)
      response.execute(post)

      (event_type, id, timestamp, /*src_ip*/ x._1, /*dst_ip*/ x._2, src_port, dst_port, protocol, /*prediction*/ x._8,
        probability, /*day*/ x._3, /*features*/ x._6.toString) // , /*time*/ x._4
    }}

    import hiveContext.implicits._
    val dateTable = anomalies.toDF("type", "id", "timestamp", "src_ip", "dst_ip", "src_port", "dst_port", "protocol", "prediction", "probability", "day", "features") // , "time"
    dateTable.write.mode("append").saveAsTable("biguardian_prod.anomalies")

    sc.stop()
  }

}
