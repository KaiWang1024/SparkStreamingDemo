package cs523.SparkConsumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.Duration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

object WebPagePopularityValueCalculator {
    private val checkpointDir = "popularity-data-checkpoint"
    private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
    
    def main(args: Array[String]) {

        val zkServer = "quickstart.cloudera:2181"
        val processingInterval = 10
        val conf = new SparkConf().setAppName("Web Page Popularity Value Calculator").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(processingInterval))
        //using updateStateByKey asks for enabling checkpoint
        ssc.checkpoint(checkpointDir)
        val kafkaStream = KafkaUtils.createStream(
        ssc,
        zkServer,
        msgConsumerGroup,
        Map("user-behavior-topic" -> 1))
        
        val msgDataRDD = kafkaStream.map(_._2)
        // e.g page37|5|1.5119122|-1
        val popularityData = msgDataRDD.map { msgLine =>
            {
                val dataArr: Array[String] = msgLine.split("\\|")
                val pageID = dataArr(0)
                //calculate the popularity value
                val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
                (pageID, popValue)
           }
        }
        //sum the previous popularity value and current value
        val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
            iterator.flatMap(t => {
                val newValue:Double = t._2.sum
                val stateValue:Double = t._3.getOrElse(0);
                Some(newValue + stateValue)
            }.map(sumedValue => (t._1, sumedValue)))
        }
        
        val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
        val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue, 
            new HashPartitioner(ssc.sparkContext.defaultParallelism), 
            true, 
            initialRDD)
            
        val tableName = "pageRate"

        //set the checkpoint interval to avoid too frequently data checkpoint which may
        //may significantly reduce operation throughput
        stateDstream.checkpoint(Duration(8*processingInterval.toInt*1000))
        
        stateDstream.foreachRDD { rdd => {
               val sortedData = rdd.map{ case (k,v) => (v,k) }.sortByKey(false)
               val topKData = sortedData.take(10).map{ case (v,k) => (k,v)}

               val hbaseConf = HBaseConfiguration.create()
               val connection = ConnectionFactory.createConnection(hbaseConf)
               val table = connection.getTable(TableName.valueOf(tableName))
               
               topKData.foreach(x => {
                   val put = new Put(Bytes.toBytes(x._1))
                   put.addColumn(Bytes.toBytes("rate"), Bytes.toBytes("score"), Bytes.toBytes(x._2.toString()))
                   table.put(put)
               })
               table.close()
               connection.close()
           }
        }
        
        ssc.start()
        ssc.awaitTermination()
    }
}