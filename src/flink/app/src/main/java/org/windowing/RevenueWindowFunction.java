package org.windowing;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.model.RevenuePerMinute;

public class RevenueWindowFunction extends ProcessWindowFunction<RevenuePerMinute, RevenuePerMinute, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<RevenuePerMinute> elements, Collector<RevenuePerMinute> out) {
        RevenuePerMinute acc = elements.iterator().next();
        long start = context.window().getStart();
        long end = context.window().getEnd();
        out.collect(new RevenuePerMinute(start, end, acc.totalRevenue, acc.tripCount));
    }
}