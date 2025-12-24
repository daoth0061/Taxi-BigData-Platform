package org.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.model.RevenuePerMinute;
import org.model.TaxiTrip;

public class RevenueAggregator implements AggregateFunction<TaxiTrip, RevenuePerMinute, RevenuePerMinute> {
    @Override
    public RevenuePerMinute createAccumulator() {
        return new RevenuePerMinute(0L, 0L, 0.0, 0L);
    }

    @Override
    public RevenuePerMinute add(TaxiTrip value, RevenuePerMinute acc) {
        acc.totalRevenue += value.totalAmount;
        acc.tripCount += 1;
        return acc;
    }

    @Override
    public RevenuePerMinute getResult(RevenuePerMinute acc) {
        return acc;
    }

    @Override
    public RevenuePerMinute merge(RevenuePerMinute a, RevenuePerMinute b) {
        return new RevenuePerMinute(0L, 0L, a.totalRevenue + b.totalRevenue, a.tripCount + b.tripCount);
    }
}