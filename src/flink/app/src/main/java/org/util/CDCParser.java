package org.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.model.TaxiTrip;

import java.time.Instant;

public class CDCParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static TaxiTrip parseDebeziumAfter(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            JsonNode after = root.get("after");
            if (after == null || after.isNull()) return null;

            String id = after.has("id") ? after.get("id").asText() : null;
            String pickupStr = after.has("tpep_pickup_datetime") ? after.get("tpep_pickup_datetime").asText() : null;
            String dropoffStr = after.has("tpep_dropoff_datetime") ? after.get("tpep_dropoff_datetime").asText() : null;

            long pickupMs = pickupStr != null ? Instant.parse(pickupStr).toEpochMilli() : System.currentTimeMillis();
            long dropoffMs = dropoffStr != null ? Instant.parse(dropoffStr).toEpochMilli() : pickupMs;

            double fare = after.has("fare_amount") ? after.get("fare_amount").asDouble() : 0.0;
            double total = after.has("total_amount") ? after.get("total_amount").asDouble() : fare;
            double plon = after.has("pickup_longitude") ? after.get("pickup_longitude").asDouble() : 0.0;
            double plat = after.has("pickup_latitude") ? after.get("pickup_latitude").asDouble() : 0.0;

            return new TaxiTrip(pickupMs, dropoffMs, fare, total, plon, plat, id);
        } catch (Exception ex) {
            // log if you have logger; return null to filter out bad events
            return null;
        }
    }
}
