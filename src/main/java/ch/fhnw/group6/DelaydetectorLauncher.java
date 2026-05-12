package ch.fhnw.group6;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * App 2 – Step 3 + 4
 *
 * Liest Timing-Events aus "group2-route-timing",
 * filtert Verspätungen > 180 Sekunden (Step 3)
 * und schreibt diese ins Dashboard-Topic "delays" (Step 4).
 *
 * Verwendet plain KafkaConsumer statt Kafka Streams (robuster).
 */
public class DelaydetectorLauncher {

    public static void main(String[] args) throws Exception {

        // ── Consumer ────────────────────────────────────────────────────────
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "192.168.111.10:9092");
        consumerProps.put("group.id", "delay-detector-group6-v1");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.offset.reset", "earliest");

        // ── Producer ─────────────────────────────────────────────────────────
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "192.168.111.10:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Partitionen manuell zuweisen (kein Group-Rebalancing nötig)
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("group6-route-timing");
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            System.out.println("FEHLER: Topic 'group6-route-timing' nicht gefunden!");
            consumer.close();
            producer.close();
            return;
        }
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            partitions.add(new TopicPartition("group6-route-timing", pi.partition()));
        }
        consumer.assign(partitions);

        // Aktuellen End-Offset ermitteln und von dort starten
        // (nicht seekToBeginning - das würde 900+ alte Messages auf einmal in delays schreiben)
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
            consumer.seek(e.getKey(), e.getValue());
            System.out.println("Starte ab Offset " + e.getValue() + " für Partition " + e.getKey().partition());
        }

        System.out.println("App 2 (DelayDetectorLauncher) gestartet.");
        System.out.println("Liest von:   group6-route-timing (nur neue Events ab jetzt)");
        System.out.println("Schreibt in: delays  (Format: id: X, delay: Y)");
        System.out.println("Dashboard:   http://192.168.111.11:8080/status/");
        System.out.println("Warte auf Nachrichten...");

        // Shutdown-Hook: wakeup() unterbricht consumer.poll() sicher im Main-Thread
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("App 2 wird gestoppt...");
            consumer.wakeup();   // ← thread-sicher; wirft WakeupException in poll()
        }));

        DelayFilter filter = new DelayFilter();
        MyProcessor processor = new MyProcessor();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();

                    // Stage 2: Filter
                    boolean significant = filter.test(key, value);

                    if (significant) {
                        // Stage 3: Peek (Logging)
                        processor.apply(key, value);

                        // Stage 4: In delays-Topic schreiben
                        // Dashboard erwartet Format: "id: 123, delay: 321"
                        // value enthält bereits dieses Format aus group6-route-timing
                        producer.send(new ProducerRecord<>("delays", key, value));
                        System.out.println("✅ In 'delays' geschrieben: key=" + key + " value=" + value);
                    }
                }
            }
        } catch (WakeupException e) {
            // Normales Shutdown-Signal – ignorieren
        } finally {
            // Consumer und Producer werden jetzt im Main-Thread geschlossen (thread-sicher)
            consumer.close();
            producer.close();
            System.out.println("App 2 gestoppt.");
        }
    }
}