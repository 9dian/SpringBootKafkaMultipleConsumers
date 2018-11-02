package com.xq.spring.boot.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaMultipleConsumptionTests {
    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaAdmin kafkaAdmin;
    @Autowired
    private Listener listener;

    @PostConstruct
    void init() {
        createTopics();
    }

    void createTopics() {
        DefaultListableBeanFactory dlBeanFactory = (DefaultListableBeanFactory) ((ConfigurableApplicationContext)applicationContext).getBeanFactory();
        NewTopic sf = new NewTopic("sf", 5, (short) 2);
        dlBeanFactory.registerSingleton("sf", sf);
        dlBeanFactory.registerSingleton("test", new NewTopic("test", 5, (short) 2));
//
//        NewTopic t = (NewTopic)applicationContext.getBean("sf");
//        System.out.println("numPartitions: "+ t.numPartitions());
        kafkaAdmin.setFatalIfBrokerNotAvailable(false);
        kafkaAdmin.setAutoCreate(false);

        kafkaAdmin.initialize();
    }

    @Test
    public void contextLoads() throws InterruptedException {


        for (int i = 0; i < 30; i++) {
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("sf", "Messsage:" + i);
            final  int ii = i;
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    System.out.println("Sent message-" + ii + ":\t" + result);
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Failed to send message");
                }
            });
        }

        assertThat(this.listener.countDownLatch0.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(this.listener.countDownLatch1.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(this.listener.countDownLatch2.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(this.listener.countDownLatch3.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(this.listener.countDownLatch4.await(10, TimeUnit.SECONDS)).isTrue();


    }

}