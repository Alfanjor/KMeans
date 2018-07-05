package Kmeans.KafkaStreaming.model

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KMeansStreamingDataSet {

  def KmeansStreamingDataSet (sc: SparkContext): Unit = {
    sc.setJobDescription("Getting KMeans_connections data")
    val ssc = new StreamingContext(sc, Seconds(300))

    val topics = "enriched"
    val brokers = "hvi1x0256:9092,hvi1x0220:9092,hvi1x0257:9092"
    val topic = topics.split(",").toSet
    val kafkaParams = Map("metadata.broker.list" -> brokers)

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
          val dateTable = hiveContext.sql("SELECT src_ip, dst_ip, " +

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
//          dateTable.orderBy("src_ip").show(100, false)
          dateTable.write.partitionBy("day").mode("append").saveAsTable("biguardian_prod.kmeans_connections")
        } else {
          val dateTable = hiveContext.sql("SELECT src_ip, dst_ip, " +

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
//          dateTable.orderBy("src_ip").show(100, false)
          dateTable.write.partitionBy("day").mode("append").saveAsTable("biguardian_prod.kmeans_connections")
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
