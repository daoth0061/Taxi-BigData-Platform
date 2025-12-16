package org.model;

public class TaxiTrip {
    public long pickupTime;    // epoch ms
    public long dropoffTime;   // epoch ms
    public double fareAmount;
    public double totalAmount;
    public double pickupLon;
    public double pickupLat;
    public String id;

    public TaxiTrip() {}

    public TaxiTrip(long pickupTime, long dropoffTime, double fareAmount, double totalAmount,
                    double pickupLon, double pickupLat, String id) {
        this.pickupTime = pickupTime;
        this.dropoffTime = dropoffTime;
        this.fareAmount = fareAmount;
        this.totalAmount = totalAmount;
        this.pickupLon = pickupLon;
        this.pickupLat = pickupLat;
        this.id = id;
    }
}
