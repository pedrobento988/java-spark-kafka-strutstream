package com.bento.javasparkkafka.spark;

import com.bento.javasparkkafka.kafka.KafkaOutputSchema;
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
                .option("startingOffsets", "earliest")
                .load();
    }

    /**
     * Initialize Streaming Query to export results into Kafka.
     *
     * @param rowDataset dataset to read from. Rows should be already in KafkaOutputSchema
     * @return Streaming query created
     */
    static StreamingQuery createKafkaProducer(Dataset<KafkaOutputSchema> rowDataset) {
        String topic = "testresult";
        String brokers = "localhost:9092";

        KafkaSink writer = new KafkaSink(topic, brokers);

        return rowDataset.writeStream()
                .foreach(writer)
                .outputMode("complete")
                .trigger(new ProcessingTime(10000))
                .start();
    }
}
