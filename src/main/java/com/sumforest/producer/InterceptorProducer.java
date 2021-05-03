package com.sumforest.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author sen
 * @date 2021/5/3 14:11
 * @description 添加拦截器异步发送生产者
 */
public class InterceptorProducer {

    public static void main(String[] args) throws InterruptedException {
        // 1.kafka生产者配置
        Properties properties = new Properties();
        // 1.1 kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092");
        // 1.2 ack
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 1.3 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 1.4 批量发送的大小 16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        // 1.5 未达到批量发送大小超时时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 1.6 配置RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 1.7 配置key序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 1.8 配置value序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("com.sumforest.interceptor.TimeInterceptor", "com.sumforest.interceptor.CounterInterceptor"));

        // 2.创建生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 3.发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "myProducer--" + i));
        }

        // 4.关闭连接，拦截器close()由此来调用
        producer.close();
    }
}
