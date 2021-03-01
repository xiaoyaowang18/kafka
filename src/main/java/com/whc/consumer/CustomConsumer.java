package com.whc.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * 高级API，自动维护消费情况
 */
public class CustomConsumer {
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "bigdata1:9092");
        //制定consumer group
        props.put("group.id", "test");
        //换组重复消费，从最开始进行消费
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //是否自动确认offset
        props.put("enable.auto.commit", "true");
        //自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //KV的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //定义consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        //消费者订阅的topic,可以同时订阅多个
        kafkaConsumer.subscribe(Arrays.asList( "second"));
        //不换组重复消费
        //kafkaConsumer.assign(Collections.singletonList(new TopicPartition("second",0)));
        //kafkaConsumer.seek(new TopicPartition("second",0),2);

        while (true) {
            //读取数据，读取超时时间100ms
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100L);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic() + "--" + record.partition() + "--" + record.value());
            }
        }

    }
}

