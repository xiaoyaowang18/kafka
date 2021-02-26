package com.whc.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "bigdata1:9092");
        //等待所有副本节点的应答
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //消息发送最大尝试次数
        props.put("retries", 0);
        //一批消息处理大小
        props.put("batch.size", 16384);
        //请求延时
        props.put("linger.ms", 1);
        //发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        //KV序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class","com.whc.producer.CustomPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>("second", String.valueOf(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        System.out.println("发送失败");
                    }
                }
            });

        producer.close();
    }
}

