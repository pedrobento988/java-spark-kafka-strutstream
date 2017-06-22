package com.bento.javasparkkafka.spark;

import com.bento.javasparkkafka.datatransformation.GenericDT;
import com.bento.javasparkkafka.datatransformation.StringToIntDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Created by bento on 22/06/2017.
 */
public class SparkFlow<T extends GenericDT> {

    private Class<T> classDT;

    public SparkFlow(Class<T> classDataTransformation) {
        classDT = classDataTransformation;
    }

    public void run() throws StreamingQueryException {
        SparkSession spark = SparkModule.createSparkSession();

        // Create DataFrame representing the stream of input data from kafka @ localhost:9092
        Dataset<Row> inputData = SparkModule.createKafkaConsumer(spark);

        Dataset<T> result = inputData.selectExpr("CAST(value AS STRING)")
                .as(Encoders.bean(classDT));

        // Initialize Output Streaming query
        // We are also converting each row into KafkaOutputSchema
        StreamingQuery query = SparkModule.createKafkaProducer(result);

        query.awaitTermination();
    }
}
