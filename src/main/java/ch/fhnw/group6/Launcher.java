package ch.fhnw.group6;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class Launcher {

    public static void main(String[] args) {

        Properties props = new Properties();

        /*
         * Möglichst nahe an Marcs Beispielcode.
         */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-position-group6-" + Math.random());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.111.10:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        /*
         * Topic aus dem Guide:
         * driver-position enthält die Positionsmeldungen.
         */
        KStream<String, String> source = builder.stream("driver-position");

        /*
         * Minimaler Test:
         * Jedes Event soll direkt auf die Konsole geschrieben werden.
         */
        source.foreach(new MyProcessor());

        Topology topology = builder.build();

        System.out.println("Kafka Streams Topology:");
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("Kafka Streams application started.");
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}