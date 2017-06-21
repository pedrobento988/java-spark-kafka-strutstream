package com.bento.javasparkkafka;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by bento on 21/06/2017.
 */
public class MainKafka {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainKafka.class);

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredSparkKafka")
                .master("local[2]")
                .getOrCreate();


        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                .load();
//        lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        lines.printSchema();

        // Split the lines into words
        Dataset<String> words = lines.selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            public Iterator<String> call(String x) {
                                return Arrays.asList(x.split(" ")).iterator();
                            }
                        }, Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();


        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
