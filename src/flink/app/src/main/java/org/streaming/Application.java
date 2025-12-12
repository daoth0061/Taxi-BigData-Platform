package org.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

import org.model.*;
import org.util.CDCParser;
import org.aggregator.RevenueAggregator;
import org.windowing.RevenueWindowFunction;

import java.time.Duration;
import java.util.Properties;

import com.datastax.driver.mapping.Mapper;

public class Application {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // ---------- Kafka Source ----------
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-taxi-analytics");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "taxi_trips_cdc",
                new SimpleStringSchema(),
                props
        );
        consumer.setStartFromLatest();

        DataStream<String> raw = env.addSource(consumer);

        // ---------- Parse CDC and assign timestamps + watermarks ----------
        WatermarkStrategy<TaxiTrip> wm = WatermarkStrategy
                .<TaxiTrip>forBoundedOutOfOrderness(Duration.ofMinutes(3))
                .withTimestampAssigner((trip, ts) -> trip.pickupTime);

        DataStream<TaxiTrip> trips = raw
                .map((MapFunction<String, TaxiTrip>) CDCParser::parseDebeziumAfter)
                .filter(t -> t != null)
                .assignTimestampsAndWatermarks(wm);

        // =========================
        // 1) Revenue per minute (tumbling) -> Cassandra
        // =========================
        DataStream<RevenuePerMinute> revenue = trips
                .keyBy(t -> 1)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new RevenueAggregator(), new RevenueWindowFunction());

        // Cassandra sink for revenue
// Standalone statement
        CassandraSink.addSink(revenue)
            .setHost("127.0.0.1",9042)
            .build();

        // =========================
        // 2) Active trips (sliding window) -> Cassandra
        // =========================
        DataStream<ActiveTrips> active = trips
                .map((MapFunction<TaxiTrip, Long>) t -> t.pickupTime)
                .keyBy(x -> 1)
                .window(SlidingEventTimeWindows.of(Time.minutes(2), Time.seconds(30)))
                .process(new ProcessWindowFunction<Long, ActiveTrips, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<Long> elements, Collector<ActiveTrips> out) {
                        long count = 0;
                        for (Long e : elements) count++;
                        out.collect(new ActiveTrips(context.window().getStart(), context.window().getEnd(), count));
                    }
                });

        // Cassandra sink for active trips
        CassandraSink.addSink(active)
                .setHost("127.0.0.1", 9042)
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();
                
        env.execute("Taxi Analytics -> Cassandra");
    }
}