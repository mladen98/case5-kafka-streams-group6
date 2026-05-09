package ch.fhnw.group6;

import org.apache.kafka.streams.kstream.ForeachAction;

public class MyProcessor implements ForeachAction<String, String> {

    @Override
    public void apply(String key, String value) {

        /*
         * Diese Ausgabe muss erscheinen, sobald Kafka Streams Events liest.
         */
        System.out.println("==================================================");
        System.out.println("MyProcessor got Event");
        System.out.println("KEY   = " + key);
        System.out.println("VALUE = " + value);
        System.out.println("==================================================");
    }
}