package ch.fhnw.group6;

import org.apache.kafka.streams.kstream.Predicate;

public class DelayFilter implements Predicate<String, String> {

    /*
     * Fachlicher Schwellwert:
     * Im Durchstich gilt eine Verzögerung ab mehr als 3 Minuten als relevant.
     */
    private static final int SIGNIFICANT_DELAY_SECONDS = 180;

    @Override
    public boolean test(String key, String value) {

        /*
         * Diese Klasse entspricht Stage 2 der Verarbeitung.
         * Sie entscheidet, ob ein RouteTimingEvent relevant genug für das Dashboard ist.
         */
        System.out.println("Stage 2 - checking delay event");
        System.out.println("Input key   = " + key);
        System.out.println("Input value = " + value);

        if (value == null) {
            return false;
        }

        /*
         * Erwartetes Format:
         * id: 123, delay: 321
         */
        Integer delay = Utils.extractDelay(value);

        if (delay == null) {
            System.out.println("Delay could not be parsed. Event ignored.");
            return false;
        }

        boolean significant = delay > SIGNIFICANT_DELAY_SECONDS;

        if (significant) {
            System.out.println("Stage 2 - significant delay detected:");
            System.out.println(value);
        } else {
            System.out.println("Stage 2 - delay not significant:");
            System.out.println(value);
        }

        return significant;
    }
}