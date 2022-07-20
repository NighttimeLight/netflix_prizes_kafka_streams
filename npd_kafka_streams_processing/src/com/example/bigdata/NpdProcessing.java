package com.example.bigdata;


import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class NpdProcessing {

    public static void main(String[] args) throws Exception {
        System.out.println("running main, args0 " + args[0]);
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "npd-application");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "npd-application-client" + String.valueOf(Math.random()));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PrizeRecordTimeExtractor.class);

        boolean daily = "daily".equals(args[1]);
        int dArg = Integer.parseInt(args[2]);
        int lArg = Integer.parseInt(args[3]);
        int oArg = Integer.parseInt(args[4]);

        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> moviesTable = builder.stream("kafka-movie-titles", Consumed.with(stringSerde, stringSerde))
                .mapValues(value -> MovieRecord.parseFromStringLine(value))
                .map((key, value) -> KeyValue.pair(String.valueOf(value.getId()), value.toString()))
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("MoviesStore"));
//        moviesTable.toStream()
//                .peek((k,v) -> System.out.println(k + " --> " + v))
//                .through("kafka-prize-alerts");

        KStream<String, PrizeRecord> prizeStream = builder.stream("kafka-netflix-prize-data", Consumed.with(stringSerde, stringSerde))
                .mapValues(value -> PrizeRecord.parseFromStringLine(value));
//        prizeStream.peek((k,v) -> System.out.println(k + " => " + v));


        KTable<Windowed<String>, Pair<Long, Long>> prizeCountsStream = prizeStream
                .map((key, value) -> KeyValue.pair(String.valueOf(value.getFilmId()), new Pair<>((long)1, (long) value.getRate())))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(dArg)))
                .reduce((aggValue, newValue) -> new Pair<>(newValue.getKey() + newValue.getKey(), aggValue.getValue() + newValue.getValue()))
                .suppress(Suppressed.untilTimeLimit(Duration.ofDays(1),Suppressed.BufferConfig.unbounded()));
//        prizeCountsStream.toStream().peek((k,v) -> System.out.println(k.toString() + " => " + v.toString()));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        KTable<String, String> alertTitles = prizeCountsStream.toStream()
                .filter((key, value) -> value.getKey() >= lArg)
                .filter((key, value) -> (value.getValue()/value.getKey()) >= oArg)
                .map((key, value) -> KeyValue.pair(key.key(),
                        "[" + sdf.format(Date.from(key.window().startTime())) + " - " + sdf.format(Date.from(key.window().endTime())) +
                                "] : " + String.valueOf(value.getKey()) + " : " + String.valueOf(value.getValue()/value.getKey())))
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue);
//        alertTitles.toStream().peek((k,v) -> System.out.println(k + " > " + v));

//        KTable<Windowed<String>, Long> prizeCountsStream = prizeStream
//                .map((key, value) -> KeyValue.pair(String.valueOf(value.getFilmId()), "|"))
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofDays(dArg)) /* time-based window */)
//                .count();
//        prizeCountsStream.toStream().peek((k,v) -> System.out.println(k.toString() + " => " + v));
//
//        KTable<String, String> alertTitles = prizeCountsStream.toStream()
//                .filter((key, value) -> value >= lArg)
//                .map((key, value) -> KeyValue.pair(key.key(), String.valueOf(value)))
//                .groupByKey()
//                .reduce((aggValue, newValue) -> newValue);
//        alertTitles.toStream().peek((k,v) -> System.out.println(k + " > " + v));

        moviesTable.toStream()
                .join(alertTitles, (movie, countAvg) -> movie + " : " + countAvg)
                .to("kafka-prize-alerts");


        Duration frequency = daily ? Duration.ofDays(1) : Duration.ofSeconds(10);


        KTable<Windowed<String>, Pair<Long, Long>> monthCountsStream = prizeStream
                .map((key, value) -> KeyValue.pair(String.valueOf(value.getFilmId()), new Pair<>((long)1, (long) value.getRate())))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .reduce((aggValue, newValue) -> new Pair<>(newValue.getKey() + newValue.getKey(), aggValue.getValue() + newValue.getValue()))
                .suppress(Suppressed.untilTimeLimit(frequency,Suppressed.BufferConfig.unbounded()));
        KTable<String, String> countSumMonth = monthCountsStream.toStream()
                .map((key, value) -> KeyValue.pair(key.key()+ " : [" +
                                sdf.format(Date.from(key.window().startTime())) + " - " + sdf.format(Date.from(key.window().endTime())) + "]",
                        String.valueOf(value.getKey()) + " : " + String.valueOf(value.getValue())))
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue);

        KTable<Windowed<String>, Long> monthUniqStream = prizeStream
                .map((key, value) -> KeyValue.pair(
                        new PrizeRecord("", String.valueOf(value.getFilmId()), String.valueOf(value.getUserId()), "").toString(),
                        (long)1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .reduce((aggValue, newValue) -> newValue)
                .suppress(Suppressed.untilTimeLimit(frequency,Suppressed.BufferConfig.unbounded()));
        KTable<String, String> countUniqMonth = monthUniqStream.toStream()
                .map((key, value) -> KeyValue.pair(String.valueOf(PrizeRecord.parseFromStringLine(key.key()).getFilmId())+ " : [" +
                                sdf.format(Date.from(key.window().startTime())) + " - " + sdf.format(Date.from(key.window().endTime())) + "]",
                        value ))
                .groupByKey()
                .reduce((aggValue, newValue) -> aggValue + newValue)
                .mapValues(value -> String.valueOf(value));

        KTable<String, String> monthAgg = countSumMonth
                .join(countUniqMonth, (countSum, uniqPrizes) -> countSum + " : " + uniqPrizes);

        monthAgg.join(alertTitles, (movie, agg) -> movie + " : " + agg,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("MonthlyAggregates"));




        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
//                streams.cleanUp();
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("started");
            latch.await();
            System.out.println("closing");
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
