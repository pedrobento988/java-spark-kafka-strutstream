package com.bento.javasparkkafka.spark;

import com.bento.javasparkkafka.datatransformation.GenericDT;
import com.bento.javasparkkafka.kafka.KafkaSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Created by bento on 22/06/2017.
 */
public class SparkModule {

    static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("JavaStructuredSparkKafka")
                .master("local[2]")
                .getOrCreate();
    }

    static Dataset<Row> createKafkaConsumer(SparkSession spark) {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "latest")
                .load();
    }

    /**
     * Initialize Streaming Query to export results into Kafka.
     *
     * @param rowDataset dataset to read from.
     * @return Streaming query created
     */
    static <T extends GenericDT> StreamingQuery createKafkaProducer(Dataset<T> rowDataset) {
        String topic = "testresult";
        String brokers = "localhost:9092";

        KafkaSink<T> writer = new KafkaSink<>(topic, brokers);

        return rowDataset.writeStream()
                .option("checkpointLocation", "/tmp/offsets")
                .foreach(writer)
                .outputMode("append")
                .trigger(new ProcessingTime(10000))
                .start();
    }
}
