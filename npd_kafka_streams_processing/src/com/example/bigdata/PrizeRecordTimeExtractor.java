package com.example.bigdata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class PrizeRecordTimeExtractor implements TimestampExtractor {

    public long extract(final ConsumerRecord<Object, Object> record,
                        final long previousTimestamp) {
        long timestamp = -1;

        String stringLine;

        if (record.value() instanceof String) {
            stringLine = (String) record.value();
            if (PrizeRecord.lineIsCorrect(stringLine)) {
                timestamp = PrizeRecord.parseFromStringLine(stringLine).getTimestampInMillis();
            }
        }
        if (timestamp < 0) {
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        }
        return timestamp;
    }
}
