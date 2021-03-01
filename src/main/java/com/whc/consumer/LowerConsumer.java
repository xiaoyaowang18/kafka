package com.whc.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据指定的topic，partition,offset来获取数据
 */
public class LowerConsumer {
    public static void main(String[] args) {
        //定义相关参数
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("bigdata1");
        brokers.add("bigdata2");
        brokers.add("bigdata3");

        //端口号
        int port = 9092;

        //主题
        String topic = "first";

        //分区
        int partition = 0;

        //offset
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,port,topic,partition,offset);

    }

    //找到分区leader
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {
            //创建获取分区leader的消费者对象
            SimpleConsumer getleader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "getleader");
            //创建一个主题元数据信息请求(可以有)
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //获取主题元数据返回值
            TopicMetadataResponse metadataResponse = getleader.send(topicMetadataRequest);
            //解析元数据返回值
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            //遍历主题元数据
            for (TopicMetadata topicMetadatum : topicsMetadata) {
                //获取多个分区的元数据信息
                List<PartitionMetadata> partitionsMetadata = topicMetadatum.partitionsMetadata();
                //遍历分区元数据
                for (PartitionMetadata partitionMetadatum : partitionsMetadata) {
                    if (partitionMetadatum.partitionId() == partition) {
                        return partitionMetadatum.leader();
                    }
                }
            }

        }

        return null;
    }

    //获取数据
    private void getData(List<String> brokers, int port, String topic, int partition, long offset) {
        //获取leader
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null) {
            return;
        }
        //获取数据的消费者对象
        SimpleConsumer getData = new SimpleConsumer(leader.host(), port, 1000, 1024 * 4, "getData");

        //创建获取数据的对象
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000).build();
        //获取数据返回值
        FetchResponse fetchResponse = getData.fetch(fetchRequest);

        //解析返回值
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        //遍历打印
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            //在这里可以保存offset到自己指定的位置
            System.out.println("此时的offset:" + offset1);
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1 + "--" + new String(bytes));
        }

    }
}
