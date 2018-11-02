package com.xq.spring.boot.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

import java.util.concurrent.CountDownLatch;

public class Listener {
    public CountDownLatch countDownLatch0 = new CountDownLatch(5);
    public CountDownLatch countDownLatch1 = new CountDownLatch(5);
    public CountDownLatch countDownLatch2 = new CountDownLatch(5);
    public CountDownLatch countDownLatch3 = new CountDownLatch(5);
    public CountDownLatch countDownLatch4 = new CountDownLatch(5);

    @KafkaListener(id = "id0", topicPartitions = { @TopicPartition(topic = "sf", partitions = { "0" }) })
    public void listenPartition0(ConsumerRecord<?, ?> record) {
        System.out.println("Listener Id0, Thread ID: " + Thread.currentThread().getId() + "\tcount: \t" + countDownLatch0.getCount() + "\n\t Received: " + record);
//        System.out.println("Received: " + record);
        countDownLatch0.countDown();

    }

    @KafkaListener(id = "id1", topicPartitions = { @TopicPartition(topic = "sf", partitions = { "1" }) })
    public void listenPartition1(ConsumerRecord<?, ?> record) {
        System.out.println("Listener Id1, Thread ID: " + Thread.currentThread().getId() + "\tcount: \t" + countDownLatch1.getCount() + "\n\t Received: " + record);
//        System.out.println("Received: " + record);
        countDownLatch1.countDown();
    }

    @KafkaListener(id = "id2", topicPartitions = { @TopicPartition(topic = "sf", partitions = { "2" }) })
    public void listenPartition2(ConsumerRecord<?, ?> record) {
        System.out.println("Listener Id2, Thread ID: " + Thread.currentThread().getId() + "\tcount: \t" + countDownLatch2.getCount() + "\n\t Received: " + record);
//        System.out.println("Received: " + record);
        countDownLatch2.countDown();
    }


    @KafkaListener(id = "id3", topicPartitions = { @TopicPartition(topic = "sf", partitions = { "3" }) })
    public void listenPartition3(ConsumerRecord<?, ?> record) {
        System.out.println("Listener Id3, Thread ID: " + Thread.currentThread().getId() + "\tcount: \t" + countDownLatch3.getCount() + "\n\t Received: " + record);
        countDownLatch3.countDown();
    }

    @KafkaListener(id = "id4", topicPartitions = { @TopicPartition(topic = "sf", partitions = { "4" }) })
    public void listenPartition4(ConsumerRecord<?, ?> record) {
        System.out.println("Listener Id4, Thread ID: " + Thread.currentThread().getId() + "\tcount: \t" + countDownLatch4.getCount() + "\n\t Received: " + record);
//        System.out.println("Received: " + record);
        countDownLatch4.countDown();
    }
}