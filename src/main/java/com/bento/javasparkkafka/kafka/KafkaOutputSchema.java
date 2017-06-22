package com.bento.javasparkkafka.kafka;

import java.io.Serializable;

public class KafkaOutputSchema implements Serializable {
    private String value;
    private long count;

    public KafkaOutputSchema() {
    }

    public String getValue() {
        return value;
    }

    public long getCount() {
        return count;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
