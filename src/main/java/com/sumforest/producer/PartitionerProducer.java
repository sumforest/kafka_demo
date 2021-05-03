package com.sumforest.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;
import java.util.Properties;

/**
 * @author sen
 * @date 2021/5/3 17:25
 * @description 使用自定义分区器生产者异步发送
 */
public class PartitionerProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.sumforest.partitioner.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first","PartitionerProducer_"+i),(recordMetadata, e) -> {
                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                if (Objects.isNull(e)) {
                    System.out.printf("分区位置：%d,offset：%d\n",recordMetadata.partition(),recordMetadata.offset());
                }else{
                    e.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
