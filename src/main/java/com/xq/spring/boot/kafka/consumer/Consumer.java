package com.xq.spring.boot.kafka.consumer;

import org.springframework.stereotype.Component;

/**
 * @Author: Xiaour
 * @Description:
 * @Date: 2018/5/22 15:03
 */
@Component
public class Consumer {

//    @KafkaListener(topics = {"test"})
//    public void listen(ConsumerRecord<?, ?> record){
//
//        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
//
//        if (kafkaMessage.isPresent()) {
//
//            Object message = kafkaMessage.get();
//            System.out.println("---->"+record);
//            System.out.println("---->"+message);
//
//        }
//
//    }
}
