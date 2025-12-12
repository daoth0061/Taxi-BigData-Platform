package org.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

@Table(keyspace = "realtime", name = "active_trips")
public class ActiveTrips implements Serializable {
    
    @PartitionKey
    @Column(name = "window_start")
    public long windowStart;
    
    @Column(name = "window_end")
    public long windowEnd;
    
    @Column(name = "active_trips")
    public long activeTrips;

    // Zero-arg constructor required
    public ActiveTrips() {}

    public ActiveTrips(long windowStart, long windowEnd, long activeTrips) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.activeTrips = activeTrips;
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

    public long getActiveTrips() {
        return activeTrips;
    }

    public void setActiveTrips(long activeTrips) {
        this.activeTrips = activeTrips;
    }
}