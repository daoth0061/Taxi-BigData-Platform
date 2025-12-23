package org.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.Date; // Crucial import

@Table(keyspace = "realtime", name = "active_trips")
public class ActiveTrips implements Serializable {
    
    @PartitionKey
    @Column(name = "window_start")
    public Date windowStart; 
    
    @Column(name = "window_end")
    public Date windowEnd;   
    
    @Column(name = "active_trips")
    public long activeTrips;

    public ActiveTrips() {}

    public ActiveTrips(long windowStart, long windowEnd, long activeTrips) {
        this.windowStart = new Date(windowStart);
        this.windowEnd = new Date(windowEnd);
        this.activeTrips = activeTrips;
    }

    public Date getWindowStart() { return windowStart; }
    public void setWindowStart(Date windowStart) { this.windowStart = windowStart; }

    public Date getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Date windowEnd) { this.windowEnd = windowEnd; }

    public long getActiveTrips() { return activeTrips; }
    public void setActiveTrips(long activeTrips) { this.activeTrips = activeTrips; }
}