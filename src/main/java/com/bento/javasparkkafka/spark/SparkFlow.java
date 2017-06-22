package com.bento.javasparkkafka.spark;

import com.bento.javasparkkafka.kafka.KafkaOutputSchema;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * Created by bento on 22/06/2017.
 */
public class SparkFlow {
    public SparkFlow() {
    }

    public void run() throws StreamingQueryException {
        SparkSession spark = SparkModule.createSparkSession();

        // Create DataFrame representing the stream of input data from kafka @ localhost:9092
        Dataset<Row> inputData = SparkModule.createKafkaConsumer(spark);



        // Split the lines into words
        Dataset<String> words = inputData.selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .flatMap(
                        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Initialize Output Streaming query
        // We are also converting each row into KafkaOutputSchema
        StreamingQuery query = SparkModule.createKafkaProducer(wordCounts.as(Encoders.bean(KafkaOutputSchema.class)));

        query.awaitTermination();
    }
}
