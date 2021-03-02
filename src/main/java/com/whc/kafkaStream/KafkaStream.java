package com.whc.kafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class KafkaStream {

    public static void main(String[] args) {
        //创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        //创建配置文件
        Properties properties = new Properties();
        properties.put("application.id","stream");
        properties.put("bootstrap.servers", "bigdata1:9092");

        //构建拓扑结构
        builder.addSource("SOURCE","w1")
                .addProcessor("PROCESSOR", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new LogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK","w2","PROCESSOR");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);

        kafkaStreams.start();


    }
}
