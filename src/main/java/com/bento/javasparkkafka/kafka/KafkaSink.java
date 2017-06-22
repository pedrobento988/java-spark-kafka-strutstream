package com.bento.javasparkkafka.kafka;


import com.bento.javasparkkafka.datatransformation.GenericDT;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.ForeachWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by bento on 22/06/2017.
 */
public class KafkaSink<T extends GenericDT> extends ForeachWriter<T> {

    private Properties kafkaProperties = new Properties();
    private KafkaProducer<String, String> producer;
    private String topic;

    public KafkaSink(String topic, String bootstrapServers) {
        kafkaProperties.put("bootstrap.servers", bootstrapServers);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Enabling retries also opens up the possibility of duplicates
        kafkaProperties.put("retries", 3);
        this.topic = topic;
    }

    public boolean open(long partitionId, long version) {
        producer = new KafkaProducer<>(kafkaProperties);
        return true;
    }

    public void process(T value) {
        producer.send(new ProducerRecord<>(topic, value.transformData()));
    }

    public void close(Throwable errorOrNull) {
        producer.close();
    }
}
