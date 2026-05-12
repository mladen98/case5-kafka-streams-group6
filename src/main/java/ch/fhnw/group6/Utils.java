package ch.fhnw.group6;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    public static final int ERROR_VALUE = -100000;

    static class GpsPos {
        String time;
        float lat;
        float lon;
    }

    private static final Pattern GPS_PATTERN =
            Pattern.compile("^id: ([0-9]+), time: (.+?), lat: (-?[0-9.]+?), lon: (-?[0-9.]+?)$");

    public static String extractId(String line) {
        if (line == null) return null;
        Matcher matcher = GPS_PATTERN.matcher(line.trim());
        if (matcher.find()) return matcher.group(1);
        return null;
    }

    public static GpsPos extractCoordinates(String line) {
        if (line == null) return null;
        Matcher matcher = GPS_PATTERN.matcher(line.trim());
        if (matcher.find()) {
            GpsPos result = new GpsPos();
            result.time = matcher.group(2);
            result.lat = Float.parseFloat(matcher.group(3));
            result.lon = Float.parseFloat(matcher.group(4));
            return result;
        }
        return null;
    }

    public static int requestDelay(String deliveryId, GpsPos pos) {
        try {
            String encodedTime = URLEncoder.encode(pos.time, StandardCharsets.UTF_8);
            URL url = new URL(
                    "http://192.168.111.11:8080/route/" + deliveryId
                            + "?time=" + encodedTime
                            + "&lat=" + pos.lat
                            + "&lon=" + pos.lon
            );

            System.out.println("Requesting delay from REST service:");
            System.out.println(url);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                System.out.println("REST service returned HTTP status: " + status);
                connection.disconnect();
                return ERROR_VALUE;
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            StringBuilder content = new StringBuilder();
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                content.append(inputLine);
            }
            reader.close();
            connection.disconnect();

            int delay = Integer.parseInt(content.toString().trim());
            System.out.println("REST service returned delay: " + delay + " seconds");
            return delay;

        } catch (Exception e) {
            e.printStackTrace();
            return ERROR_VALUE;
        }
    }

    private static final Pattern DELAY_PATTERN =
            Pattern.compile("^id: ([0-9]+), delay: ([0-9-]+?)$");

    public static Integer extractDelay(String line) {
        if (line == null) return null;
        Matcher matcher = DELAY_PATTERN.matcher(line.trim());
        if (matcher.find()) return Integer.parseInt(matcher.group(2));
        return null;
    }
}