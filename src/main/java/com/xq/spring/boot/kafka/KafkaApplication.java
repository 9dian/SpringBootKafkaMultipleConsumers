package com.xq.spring.boot.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
        DefaultListableBeanFactory dlBeanFactory = (DefaultListableBeanFactory) context.getBeanFactory();

//        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(NewTopic.class);
//        // add dependency to other bean
////        builder.addPropertyReference("propertyName", "someBean");
//        builder.addPropertyValue("name", "sf");
//        builder.addPropertyValue("numPartitions", 5);
//        builder.addPropertyValue("replicationFactor", new Short((short) 2));
//        dlBeanFactory.registerBeanDefinition("sf", builder.getBeanDefinition());

        NewTopic sf = new NewTopic("sf", 5, (short) 2);
        dlBeanFactory.registerSingleton("sf", sf);

        NewTopic t = (NewTopic)context.getBean("sf");

        System.out.println("numPartitions: "+ t.numPartitions());


//        context.getBeanFactory()
    }


//	@Bean
//	public NewTopic topic1() {
//        return new NewTopic("sf", 5, (short) 2);
//	}

	@Bean
	public NewTopic topicTest() {
		return new NewTopic("test", 5, (short) 2);
	}

	@Bean
	@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
		configurer.configure(factory, kafkaConsumerFactory);

//		factory.setConcurrency(5);
//		factory.getContainerProperties().setPollTimeout(3000);

		return factory;
	}
//
//	@Bean
//	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
//		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//		factory.setConsumerFactory(consumerFactory());
//		factory.setConcurrency(5);
//		factory.getContainerProperties().setPollTimeout(3000);
//		return factory;
//	}

//	@Bean
//	public ConsumerFactory<String, String> consumerFactory() {
//		return new DefaultKafkaConsumerFactory(Collections.emptyMap());
//	}

	@Bean
	public Listener listener() {
		return new Listener();
	}
}
