package ch.fhnw.group6;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class TopicInspector {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.10:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // ── Topic 1: driver-position ──────────────────────────────────────────
        inspectDriverPosition(consumer);

        System.out.println("\n\n");

        // ── Topic 2: group6-route-timing ──────────────────────────────────────
        inspectRouteTiming(consumer);

        System.out.println("\n\n");

        // ── Topic 3: delays ───────────────────────────────────────────────────
        inspectDelays(consumer);

        consumer.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Topic 1 – driver-position
    // ─────────────────────────────────────────────────────────────────────────
    private static void inspectDriverPosition(KafkaConsumer<String, String> consumer) {
        String topic = "driver-position";
        System.out.println("════════════════════════════════════════════════════");
        System.out.println("  TOPIC 1: " + topic);
        System.out.println("════════════════════════════════════════════════════");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            System.out.println("❌ Topic '" + topic + "' nicht gefunden oder keine Partitionen!");
            return;
        }

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            partitions.add(new TopicPartition(topic, pi.partition()));
        }
        consumer.assign(partitions);

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        long totalMessages = 0;
        for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
            long end = e.getValue();
            totalMessages += end;
            long target = Math.max(0, end - 5);
            consumer.seek(e.getKey(), target);
            System.out.println("Partition " + e.getKey().partition() + ": " + end + " Nachrichten total, lese ab Offset " + target);
        }
        System.out.println("Gesamt im Topic: ~" + totalMessages + " Nachrichten\n");

        if (totalMessages == 0) {
            System.out.println("❌ Das Topic '" + topic + "' ist LEER!");
            return;
        }

        System.out.println("=== LETZTE NACHRICHTEN AUS " + topic + " ===\n");
        int count = 0;
        long start = System.currentTimeMillis();
        while (count < 10 && System.currentTimeMillis() - start < 10000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                count++;
                System.out.println("--- Nachricht " + count + " (Partition " + record.partition() + ", Offset " + record.offset() + ") ---");
                System.out.println("KEY:   " + record.key());
                System.out.println("VALUE: " + record.value());

                String id = Utils.extractId(record.value());
                Utils.GpsPos pos = Utils.extractCoordinates(record.value());

                if (pos == null || id == null) {
                    System.out.println("⚠️  Regex passt NICHT! Format passt nicht zu:");
                    System.out.println("    'id: X, time: ..., lat: X, lon: X'");
                } else {
                    System.out.println("✅  Regex OK - ID=" + id + "  time='" + pos.time + "'");
                    String encodedTime = java.net.URLEncoder.encode(pos.time, java.nio.charset.StandardCharsets.UTF_8);
                    String url = "http://192.168.111.11:8080/route/" + id + "?time=" + encodedTime + "&lat=" + pos.lat + "&lon=" + pos.lon;
                    System.out.println("    REST-Aufruf: " + url);

                    int delay = Utils.requestDelay(id, pos);
                    if (delay == Utils.ERROR_VALUE) {
                        System.out.println("❌  REST-Aufruf FEHLGESCHLAGEN! (Zeitformat falsch oder Service-Fehler)");
                    } else {
                        System.out.println("✅  Delay = " + delay + " Sekunden " +
                                (delay > 180 ? "→ ERSCHEINT IM DASHBOARD! 🎉" : "→ wird herausgefiltert (≤180s)"));
                    }
                }
                System.out.println();
            }
        }

        if (count == 0) System.out.println("❌ Keine Nachrichten gelesen (Timeout).");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Topic 2 – group6-route-timing
    // ─────────────────────────────────────────────────────────────────────────
    private static void inspectRouteTiming(KafkaConsumer<String, String> consumer) {
        String topic = "group6-route-timing";
        System.out.println("════════════════════════════════════════════════════");
        System.out.println("  TOPIC 2: " + topic);
        System.out.println("════════════════════════════════════════════════════");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            System.out.println("❌ Topic '" + topic + "' nicht gefunden oder keine Partitionen!");
            System.out.println("   → App 1 (RouteTimingLauncher) hat noch nichts geschrieben oder Topic existiert nicht.");
            return;
        }

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            partitions.add(new TopicPartition(topic, pi.partition()));
        }
        consumer.assign(partitions);

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        long totalMessages = 0;
        for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
            long end = e.getValue();
            totalMessages += end;
            long target = Math.max(0, end - 5);
            consumer.seek(e.getKey(), target);
            System.out.println("Partition " + e.getKey().partition() + ": " + end + " Nachrichten total, lese ab Offset " + target);
        }
        System.out.println("Gesamt im Topic: ~" + totalMessages + " Nachrichten\n");

        if (totalMessages == 0) {
            System.out.println("❌ Das Topic '" + topic + "' ist LEER!");
            System.out.println("   → App 1 (RouteTimingLauncher) läuft entweder nicht,");
            System.out.println("     oder der REST-Service antwortet nicht (alle Events werden herausgefiltert).");
            return;
        }

        System.out.println("=== LETZTE NACHRICHTEN AUS " + topic + " ===\n");
        int count = 0;
        long start = System.currentTimeMillis();
        while (count < 10 && System.currentTimeMillis() - start < 10000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                count++;
                System.out.println("--- Nachricht " + count + " (Partition " + record.partition() + ", Offset " + record.offset() + ") ---");
                System.out.println("KEY:   " + record.key());
                System.out.println("VALUE: " + record.value());

                Integer delay = Utils.extractDelay(record.value());
                if (delay == null) {
                    System.out.println("⚠️  Regex passt NICHT! Erwartetes Format: 'id: X, delay: Y'");
                } else {
                    System.out.println("✅  Delay = " + delay + " Sekunden " +
                            (delay > 180
                                    ? "→ wird von DelayFilter DURCHGELASSEN → ins 'delays'-Topic! 🎉"
                                    : "→ wird von DelayFilter herausgefiltert (≤180s)"));
                }
                System.out.println();
            }
        }

        if (count == 0) System.out.println("❌ Keine Nachrichten gelesen (Timeout).");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Topic 3 – delays
    // ─────────────────────────────────────────────────────────────────────────
    private static void inspectDelays(KafkaConsumer<String, String> consumer) {
        String topic = "delays";
        System.out.println("════════════════════════════════════════════════════");
        System.out.println("  TOPIC 3: " + topic);
        System.out.println("════════════════════════════════════════════════════");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            System.out.println("❌ Topic '" + topic + "' nicht gefunden!");
            return;
        }

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            partitions.add(new TopicPartition(topic, pi.partition()));
        }
        consumer.assign(partitions);

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        long totalMessages = 0;
        for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
            long end = e.getValue();
            totalMessages += end;
            long target = Math.max(0, end - 10);
            consumer.seek(e.getKey(), target);
            System.out.println("Partition " + e.getKey().partition() + ": " + end + " Nachrichten total, lese ab Offset " + target);
        }
        System.out.println("Gesamt im Topic: ~" + totalMessages + " Nachrichten\n");

        if (totalMessages == 0) {
            System.out.println("❌ Das Topic 'delays' ist LEER! → App 2 schreibt nichts.");
            return;
        }

        System.out.println("=== ERSTE 5 NACHRICHTEN AUS " + topic + " (andere Gruppen) ===\n");
        // Zum Anfang springen und erste 5 Nachrichten lesen
        for (TopicPartition tp : partitions) {
            consumer.seek(tp, 0);
        }
        int countFirst = 0;
        long startFirst = System.currentTimeMillis();
        outerFirst:
        while (System.currentTimeMillis() - startFirst < 8000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                if (countFirst >= 5) break outerFirst;
                countFirst++;
                System.out.println("--- Erste Nachricht " + countFirst + " (Offset " + record.offset() + ") ---");
                System.out.println("KEY:   '" + record.key() + "'");
                System.out.println("VALUE: '" + record.value() + "'");
                System.out.println("Timestamp: " + new java.util.Date(record.timestamp()));
                System.out.println();
            }
            if (countFirst >= 5) break;
        }

        // Wieder zum Ende zurück für die letzten Nachrichten
        for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
            long target = Math.max(0, e.getValue() - 10);
            consumer.seek(e.getKey(), target);
        }

        System.out.println("=== LETZTE NACHRICHTEN AUS " + topic + " ===\n");
        int count = 0;
        long start = System.currentTimeMillis();

        while (count < 10 && System.currentTimeMillis() - start < 8000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                count++;
                System.out.println("--- Nachricht " + count + " (Partition " + record.partition() + ", Offset " + record.offset() + ") ---");
                System.out.println("KEY:   '" + record.key() + "'");
                System.out.println("VALUE: '" + record.value() + "'");
                System.out.println("Timestamp: " + new java.util.Date(record.timestamp()));
                System.out.println();
            }
        }
        if (count == 0) System.out.println("❌ Keine Nachrichten gelesen (Timeout).");
    }
}
