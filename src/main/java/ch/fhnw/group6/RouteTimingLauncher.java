package ch.fhnw.group6;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class RouteTimingLauncher {

    public static void main(String[] args) {

        // Consumer-Konfiguration (Plain KafkaConsumer – kein Kafka Streams, kein REBALANCING)
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "192.168.111.10:9092");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "latest");

        // Producer-Konfiguration
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "192.168.111.10:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Manuelle Partition-Zuweisung → kein Group-Coordinator, kein REBALANCING
        TopicPartition partition = new TopicPartition("driver-position", 0);
        List<TopicPartition> partitions = Collections.singletonList(partition);
        consumer.assign(partitions);

        // Nur neue Nachrichten lesen (nicht 2.3 Mio. alte)
        consumer.seekToEnd(partitions);

        System.out.println("App 1 (RouteTimingLauncher) gestartet.");
        System.out.println("Liest von:   driver-position (Partition 0, ab Ende)");
        System.out.println("Schreibt in: group6-route-timing");
        System.out.println("Warte auf neue GPS-Events ...");

        RouteTimingMapper mapper = new RouteTimingMapper();
        long processedCount = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown: " + processedCount + " Events verarbeitet.");
            producer.flush();
            producer.close();
        }));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KeyValue<String, String> result = mapper.apply(record.key(), record.value());

                    if (result.value != null) {
                        producer.send(new ProducerRecord<>("group6-route-timing", result.key, result.value));
                        System.out.println("→ Geschrieben nach group6-route-timing: " + result.value);
                    }
                } catch (Exception e) {
                    System.err.println("Fehler beim Verarbeiten von Record key=" + record.key() + ": " + e.getMessage());
                }
            }
        }
    }
}