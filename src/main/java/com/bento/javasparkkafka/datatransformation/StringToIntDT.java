package com.bento.javasparkkafka.datatransformation;

/**
 * Created by bento on 22/06/2017.
 */
public class StringToIntDT implements GenericDT {
    private String value;

    public StringToIntDT() {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        // also do whatever you want and save it in internal state
        this.value = value;
    }

    @Override
    public String transformData() {
        try {
            // Make sure it is an integer, but if it is, keep it a String
            return Integer.valueOf(value).toString();
        } catch (NumberFormatException ignored) {
            // For now, don't let it blow (catch...)
            return "0";
        }
    }
}
