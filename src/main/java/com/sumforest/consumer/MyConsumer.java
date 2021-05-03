package com.sumforest.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author sen
 * @date 2021/5/3 18:09
 * @description 消费者
 */
public class MyConsumer {

    public static void main(String[] args) {
        // 1.创建配置文件
        Properties properties = new Properties();
        // 配置kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092");

       /* // 开启自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 这是自动提交offset间隔时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);*/

        // 关闭自动提交offset，消费将不会跟新offest到kafka集群，低版本更新zookeeper，每次同一个消费者组启动将会从之前kafka或者zookeeper中的offset中开始
        // 消费
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 这是消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my_consumer");

        // 修改默认重新设置offset的策略，默认是latest。启动新的消费者组如果设置的策略是earliest，并且kafka集群中没有这个消费者组的offset记录或者原来记录已经
        // 失效那么这个消费者组将会从新消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 设置key，value反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 2.创建消费者实例
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3.订阅topic,消费者订阅不存在的topic时会自动创建
        consumer.subscribe(Arrays.asList("first","fifth"));

        // 4.循环消费
        while (true) {
            // 设置拉取消息为空时，延迟拉佢时间
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
            }
        }
    }
}
