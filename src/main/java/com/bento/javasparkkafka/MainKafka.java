package com.bento.javasparkkafka;

import com.bento.javasparkkafka.kafka.KafkaOutputSchema;
import com.bento.javasparkkafka.spark.SparkFlow;
import com.bento.javasparkkafka.spark.SparkModule;
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

/**
 * Created by bento on 21/06/2017.
 */
public class MainKafka {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainKafka.class);

    public static void main(String[] args) throws StreamingQueryException {
        new SparkFlow().run();
    }
}
