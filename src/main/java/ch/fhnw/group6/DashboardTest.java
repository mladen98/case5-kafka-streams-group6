package ch.fhnw.group6;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

/**
 * Direkter Dashboard-Test:
 * Schreibt eine Testnachricht in 'delays' und prüft sofort das Dashboard.
 */
public class DashboardTest {

    public static void main(String[] args) throws Exception {

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "192.168.111.10:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);


        System.out.println("══════════════════════════════════════════");
        System.out.println("DASHBOARD-TEST (30 Nachrichten, 30 Sek)");
        System.out.println("══════════════════════════════════════════");

        // 30 Nachrichten im Abstand von 1 Sekunde schreiben
        new Thread(() -> {
            try {
                for (int i = 1; i <= 30; i++) {
                    String key   = "9999";
                    String value = "id: 9999, delay: 999";
                    producer.send(new ProducerRecord<>("delays", key, value));
                    producer.flush();
                    System.out.println("[Producer] Nachricht " + i + "/30 geschrieben: " + value);
                    Thread.sleep(1000);
                }
                producer.close();
            } catch (Exception e) { e.printStackTrace(); }
        }).start();

        // Gleichzeitig 30x das Dashboard prüfen
        System.out.println("Frage Dashboard ab (30x, je 1 Sekunde)...");
        for (int i = 1; i <= 30; i++) {
            Thread.sleep(1000);
            String html = getDashboard();
            boolean found = html.contains("9999");
            System.out.println("Dashboard-Abfrage " + i + ": " + (found ? "✅ 9999 SICHTBAR!" : "❌ leer"));
            if (found) {
                System.out.println("=== DASHBOARD HTML ===");
                System.out.println(html);
                break;
            }
        }
        System.out.println("Test fertig.");
    }

    private static String getDashboard() throws Exception {
        URL url = new URL("http://192.168.111.11:8080/status/");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) sb.append(line);
        reader.close();
        conn.disconnect();
        return sb.toString();
    }
}

