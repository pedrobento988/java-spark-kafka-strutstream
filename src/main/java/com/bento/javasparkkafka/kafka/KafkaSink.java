package com.bento.javasparkkafka.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.ForeachWriter;

import java.util.Properties;

/**
 * Created by bento on 22/06/2017.
 */
public class KafkaSink extends ForeachWriter<KafkaOutputSchema> {
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
        producer = new KafkaProducer<String, String>(kafkaProperties);
        return true;
    }

    public void process(KafkaOutputSchema value) {
        // TODO improve: key is being saved as value! Could use another ProducerRecord constructor to separate these
        producer.send(new ProducerRecord<String, String>(topic, value.getValue() + ":" + value.getCount()));
    }

    public void close(Throwable errorOrNull) {
        producer.close();
    }
}
