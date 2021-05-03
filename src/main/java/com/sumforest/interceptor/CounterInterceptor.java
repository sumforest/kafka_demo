package com.sumforest.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author sen
 * @date 2021/5/3 22:51
 * @description
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private final AtomicInteger success = new AtomicInteger(0);

    private final AtomicInteger error = new AtomicInteger(0);

    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * @param record
     * @return 一定要return ProducerRecord
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (Objects.nonNull(metadata)) {
            success.getAndIncrement();
        } else {
            error.getAndIncrement();
        }
    }

    @Override
    public void close() {
        System.out.println("success:" + success.getAndIncrement());
        System.out.println("error:" + error.getAndIncrement());
    }
}
