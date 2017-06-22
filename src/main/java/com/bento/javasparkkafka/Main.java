package com.bento.javasparkkafka;

import com.bento.javasparkkafka.datatransformation.StringToIntDT;
import com.bento.javasparkkafka.spark.SparkFlow;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bento on 21/06/2017.
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws StreamingQueryException {
        new SparkFlow<>(StringToIntDT.class).run();
    }
}
