package org.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.Date; // Use java.util.Date

@Table(keyspace = "realtime", name = "revenue_minute")
public class RevenuePerMinute implements Serializable {
    
    @PartitionKey
    @Column(name = "window_start")
    public Date windowStart; 
    
    @Column(name = "window_end")
    public Date windowEnd;  
    
    @Column(name = "total_revenue")
    public double totalRevenue;
    
    @Column(name = "trip_count")
    public long tripCount;

    public RevenuePerMinute() {}

    public RevenuePerMinute(long windowStart, long windowEnd, double totalRevenue, long tripCount) {
        this.windowStart = new Date(windowStart);
        this.windowEnd = new Date(windowEnd);
        this.totalRevenue = totalRevenue;
        this.tripCount = tripCount;
    }

    public Date getWindowStart() { return windowStart; }
    public void setWindowStart(Date windowStart) { this.windowStart = windowStart; }

    public Date getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Date windowEnd) { this.windowEnd = windowEnd; }

    public double getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(double totalRevenue) { this.totalRevenue = totalRevenue; }

    public long getTripCount() { return tripCount; }
    public void setTripCount(long tripCount) { this.tripCount = tripCount; }
}