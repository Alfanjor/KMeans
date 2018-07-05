package Kmeans

import Math.sqrt
import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.serializer.StringDecoder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KMeansPrediction {

  def KmeansPredictionStreaming(sc: SparkContext): Unit = {
    val ssc = new StreamingContext(sc, Seconds(300))

    val topics = "enriched"
    val brokers = "hvi1x0256:9092,hvi1x0220:9092,hvi1x0257:9092"
    val topic = topics.split(",").toSet
    val kafkaParams = Map("metadata.broker.list" -> brokers)

    val kmeansModel = org.apache.spark.mllib.clustering.KMeansModel.load(sc, "hdfs://hvi1x0220:8020/user/biguardian/KMeans/KMeans_Model_K11_p92")

    sc.setJobDescription("Kmeans_Prediction")
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    //    hiveContext.sql("use biguardian_prod")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    val eventsDStream = messages.map(pair => pair._2).filter(line => line.contains("\"sensor_type\":\"netflow\""))

    eventsDStream.foreachRDD{ rdd =>
      if (!rdd.isEmpty()) {

        val now = Calendar.getInstance().getTime()
        val timeFormat = new SimpleDateFormat("HH:mm:00")
        val tsegm_initTime = timeFormat.format(now)

        val df = hiveContext.read.json(rdd)
        df.na.fill(0L)
        df.registerTempTable("raw_events")
        var stats = false

        try {
          df.select("netflow_exported_record_count")
          stats = true
        } catch {
          case e: Exception =>
            stats = false
        }

        if (stats) {
          val dataTable = hiveContext.sql("SELECT src_ip, dst_ip, " +

            "TO_DATE(last(end_time)) AS day, " +
            "'"+tsegm_initTime+"' AS tsegm_initTime, " +
            "from_unixtime(unix_timestamp(last(end_time),'yyyy-MM-dd'),'E') AS week_day, " +

            "COUNT(1) AS n_events, " +
            "COUNT(DISTINCT src_port) AS n_src_port, " +
            "COUNT(DISTINCT dst_port) AS n_dst_port, " +
            "COUNT(case when protocol_name = 'TCP' then 1 end) AS n_tcp_protocol, " +
            "COUNT(case when protocol_name = 'UDP' then 1 end) AS n_udp_protocol, " +
            "COUNT(case when protocol_name = 'ICMP' then 1 end) AS n_icmp_protocol, " +
            "COUNT(case when protocol_name = 'UNKNOWN' then 1 end) AS n_unknown_protocol, " +

            "SUM(netflow_octet_count) AS n_octetos, " +
            "SUM(netflow_reverse_octet_count) AS n_octetos_reverse, " +
            "SUM(netflow_packet_count) AS n_paquets, " +
            "SUM(netflow_reverse_packet_count) AS n_paquets_reverse, " +

            "COUNT(case when netflow_flow_end_reason = 1 then 1 end) AS n_idle_timeout_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 2 then 1 end) AS n_active_timeout_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 3 then 1 end) AS n_end_of_flow_detected_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 4 then 1 end) AS n_forced_end_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 5 then 1 end) AS n_lack_of_resources_endReason, " +

            "SUM(netflow_exported_record_count) as n_exported, " +
            "SUM(netflow_dropped_packet_count) as n_dropped, " +
            "SUM(netflow_ignored_packet_count) as n_ignored, " +
            "SUM(netflow_tcp_urg_count) as n_tcp_urg_count " +

            "FROM raw_events GROUP BY src_ip, dst_ip"
          ).na.fill(0L)
//          dataTable.show()
          prediction(sc, hiveContext, dataTable, kmeansModel)
        } else {
          val dataTable = hiveContext.sql("SELECT src_ip, dst_ip, " +

            "TO_DATE(last(end_time)) AS day, " +
            "'"+tsegm_initTime+"' AS tsegm_initTime, " +
            "from_unixtime(unix_timestamp(last(end_time),'yyyy-MM-dd'),'E') AS week_day, " +

            "COUNT(1) AS n_events, " +
            "COUNT(DISTINCT src_port) AS n_src_port, " +
            "COUNT(DISTINCT dst_port) AS n_dst_port, " +
            "COUNT(case when protocol_name = 'TCP' then 1 end) AS n_tcp_protocol, " +
            "COUNT(case when protocol_name = 'UDP' then 1 end) AS n_udp_protocol, " +
            "COUNT(case when protocol_name = 'ICMP' then 1 end) AS n_icmp_protocol, " +
            "COUNT(case when protocol_name = 'UNKNOWN' then 1 end) AS n_unknown_protocol, " +

            "SUM(netflow_octet_count) AS n_octetos, " +
            "SUM(netflow_reverse_octet_count) AS n_octetos_reverse, " +
            "SUM(netflow_packet_count) AS n_paquets, " +
            "SUM(netflow_reverse_packet_count) AS n_paquets_reverse, " +

            "COUNT(case when netflow_flow_end_reason = 1 then 1 end) AS n_idle_timeout_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 2 then 1 end) AS n_active_timeout_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 3 then 1 end) AS n_end_of_flow_detected_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 4 then 1 end) AS n_forced_end_endReason, " +
            "COUNT(case when netflow_flow_end_reason = 5 then 1 end) AS n_lack_of_resources_endReason " +

            "FROM raw_events GROUP BY src_ip, dst_ip"
          ).withColumn("n_exported", lit(0L))
            .withColumn("n_dropped", lit(0L))
            .withColumn("n_ignored", lit(0L))
            .withColumn("n_tcp_urg_count", lit(0L))
//          dataTable.show()
          prediction(sc, hiveContext, dataTable, kmeansModel)
        }

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  private def prediction (sc: SparkContext, hiveContext: HiveContext, dataTable: DataFrame, kmeansModel: KMeansModel): Unit = {

    def distance (a: org.apache.spark.mllib.linalg.Vector, b: org.apache.spark.mllib.linalg.Vector) = sqrt(Vectors.sqdist(a,b))
    val path = "hdfs://hvi1x0220:8020/user/biguardian/KMeans/"

    val dataRaw = dataTable.rdd.map{x => {
      (x.getString(0),         // src_ip
        x.getString(1),        // dst_ip
        x.getDate(2).toString, // day
        x.getString(3),        // tsegm_initTime
        x.getString(4),        // week_day

        Vectors.dense(x.getLong(5), // n_events
          x.getLong(6),  // n_src_port
          x.getLong(7),  // n_dst_port
          x.getLong(8),  // n_tcp_protocol
          x.getLong(9),  // n_udp_protocol
          x.getLong(10), // n_icmp_protocl
          x.getLong(11), // n_unknown_protocol

          x.getLong(12), // n_octetos
          x.getLong(13), // n_octetos_reverse
          x.getLong(14), // n_paquets
          x.getLong(15), // n_paquets_reverse

          x.getLong(16), // n_idle_timeout_endReason
          x.getLong(17), // n_active_timeout_endReason
          x.getLong(18), // n_end_of_flow_detected_endReason
          x.getLong(19), // n_forced_end_endReason
          x.getLong(20), // n_lack_of_reseources_endReason

          x.getLong(21), // n_exported
          x.getLong(22), // n_dropped
          x.getLong(23), // n_ignored
          x.getLong(24)  // n_tcp_urg_count
        )
      )
    }}.coalesce(40).persist()

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

    val centers = kmeansModel.clusterCenters
    val distancesPrediction = dataNormalized.map{ x => {
      val prediction = kmeansModel.predict(x._7)
      val distanceToCenter = distance(x._7, centers(prediction))
      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,prediction,distanceToCenter)
    }}

    aux = hiveContext.read.load(path + "KMeans_Boundarys_p92").map(x => x(1).toString)
    val boundarysList = aux.collect()

    // ANOMALY FILTER AND SOLR/HIVE SINK
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

//    val muestra = distancesPrediction.map{x =>
//      var anomaly = false
//      if (x._9 > boundarysList(x._8)) {
//        anomaly = true
//      }
//      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,boundarysList(x._8),anomaly)
//    }
//    muestra.toDF("src_ip","dst_ip","tsegm_inittime","week_day","day","features","normalizedFeatures","predictions","distances","bondary","anomaly")
//        .coalesce(40).write.save(path + "Batch_Muestra_test")

  }
}
