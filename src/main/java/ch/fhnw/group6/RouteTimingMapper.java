package ch.fhnw.group6;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class RouteTimingMapper implements KeyValueMapper<String, String, KeyValue<String, String>> {

    @Override
    public KeyValue<String, String> apply(String key, String value) {

        /*
         * Diese Methode entspricht Stage 1 der Verarbeitung.
         * Sie wird für jedes Event aus dem Topic driver-position aufgerufen.
         */
        System.out.println("Stage 1 - processing driver-position event");
        System.out.println("Input key   = " + key);
        System.out.println("Input value = " + value);

        /*
         * GPS-Daten aus dem Event lesen.
         */
        Utils.GpsPos position = Utils.extractCoordinates(value);

        if (position == null) {
            System.out.println("GPS position could not be parsed. Event ignored.");
            return new KeyValue<>(key, null);
        }

        /*
         * Delivery-ID bestimmen.
         * Je nach Kafka-Message steht sie im Key oder im Value.
         */
        String deliveryId = key;

        if (deliveryId == null || deliveryId.isBlank()) {
            deliveryId = Utils.extractId(value);
        }

        if (deliveryId == null || deliveryId.isBlank()) {
            System.out.println("Delivery ID could not be extracted. Event ignored.");
            return new KeyValue<>(key, null);
        }

        /*
         * Kontextanreicherung:
         * Der REST-Service berechnet die Verzögerung auf Basis von Route, Zeit und GPS-Position.
         */
        int delayInSeconds = Utils.requestDelay(deliveryId, position);

        if (delayInSeconds == Utils.ERROR_VALUE) {
            System.out.println("Delay could not be requested. Event ignored.");
            return new KeyValue<>(deliveryId, null);
        }

        /*
         * Neues Event erzeugen.
         * Das Format entspricht auch dem Format, das das Dashboard erwartet.
         */
        String routeTimingEvent = "id: " + deliveryId + ", delay: " + delayInSeconds;

        System.out.println("Stage 1 - created RouteTimingEvent:");
        System.out.println(routeTimingEvent);

        return new KeyValue<>(deliveryId, routeTimingEvent);
    }
}