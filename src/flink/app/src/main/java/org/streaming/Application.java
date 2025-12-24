package org.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

import org.model.*;
import org.util.CDCParser;
import org.aggregator.RevenueAggregator;
import org.windowing.RevenueWindowFunction;

import java.time.Duration;

import com.datastax.driver.mapping.Mapper;

public class Application {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("taxi.public.taxi_trips")
                .setGroupId("flink-taxi-analytics")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        WatermarkStrategy<TaxiTrip> wm = WatermarkStrategy
                .<TaxiTrip>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((trip, ts) -> trip.pickupTime)
                .withIdleness(Duration.ofSeconds(20));

        DataStream<TaxiTrip> trips = raw
                .map((MapFunction<String, TaxiTrip>) CDCParser::parseDebeziumAfter)
                .filter(t -> t != null)
                .assignTimestampsAndWatermarks(wm);


        DataStream<RevenuePerMinute> revenue = trips
                .keyBy(t -> 1)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new RevenueAggregator(), new RevenueWindowFunction());

        // Cassandra sink for revenue
        CassandraSink.addSink(revenue)
                .setHost("cassandra", 9042)
                .build();


        DataStream<ActiveTrips> active = trips
                .map((MapFunction<TaxiTrip, Long>) t -> t.pickupTime)
                .keyBy(x -> 1)
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(2), Duration.ofSeconds(30)))
                .process(new ProcessWindowFunction<Long, ActiveTrips, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<Long> elements, Collector<ActiveTrips> out) {
                        long count = 0;
                        for (Long ignored : elements) count++;
                        out.collect(new ActiveTrips(
                                context.window().getStart(),
                                context.window().getEnd(),
                                count
                        ));
                    }
                });

        // Cassandra sink for active trips
        CassandraSink.addSink(active)
                .setHost("cassandra", 9042)
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();

        env.execute("Taxi Analytics -> Cassandra");
    }
}