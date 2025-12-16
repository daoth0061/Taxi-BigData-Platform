package org.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

@Table(keyspace = "realtime", name = "revenue_minute")
public class RevenuePerMinute implements Serializable {
    
    @PartitionKey
    @Column(name = "window_start")
    public long windowStart;
    
    @Column(name = "window_end")
    public long windowEnd;
    
    @Column(name = "total_revenue")
    public double totalRevenue;
    
    @Column(name = "trip_count")
    public long tripCount;

    // Zero-arg constructor required by Flink serialization and Cassandra mapper
    public RevenuePerMinute() {}

    public RevenuePerMinute(long windowStart, long windowEnd, double totalRevenue, long tripCount) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalRevenue = totalRevenue;
        this.tripCount = tripCount;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public long getTripCount() {
        return tripCount;
    }

    public void setTripCount(long tripCount) {
        this.tripCount = tripCount;
    }
}