package org.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.model.TaxiTrip;


public class CDCParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static TaxiTrip parseDebeziumAfter(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            
            String id = root.has("id") ? root.get("id").asText() : null;

            long pickupMs = root.has("tpep_pickup_datetime") 
                    ? root.get("tpep_pickup_datetime").asLong() 
                    : System.currentTimeMillis();
                    
            long dropoffMs = root.has("tpep_dropoff_datetime") 
                    ? root.get("tpep_dropoff_datetime").asLong() 
                    : pickupMs;

            double fare = root.has("fare_amount") ? root.get("fare_amount").asDouble() : 0.0;
            double total = root.has("total_amount") ? root.get("total_amount").asDouble() : fare;
            double plon = root.has("pickup_longitude") ? root.get("pickup_longitude").asDouble() : 0.0;
            double plat = root.has("pickup_latitude") ? root.get("pickup_latitude").asDouble() : 0.0;

            return new TaxiTrip(pickupMs, dropoffMs, fare, total, plon, plat, id);
        } catch (Exception ex) {
            return null; 
        }
    }
}
