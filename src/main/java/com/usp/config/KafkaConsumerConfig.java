package com.usp.config;

import com.usp.engine.Producer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private static final Logger logger= LoggerFactory.getLogger(KafkaConsumerConfig.class);


    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${order-submission.group-id}")
    private String orderFulfillmentGroupId;

    @Value(value = "${order-submission.group-id-dlt}")
    private String orderFulfillmentDTLGroupId;

    @Value("${order-submission.topic-name-dlt}")
    private String topicDltName;


    @Value("${order-submission.num-partitions}")
    private String numPartitions;
    @Value("${order-submission.num-replicas}")
    private String numReplicas;

    @Value("${order-submission.retry-policy.max-attempts}")
    private String maxAttempts;
    @Value("${order-submission.backoff-policy.initial-interval}")
    private String initialInterval;
    @Value("${order-submission.backoff-policy.multiplier}")
    private String multiplier;
    @Value("${order-submission.backoff-policy.max-interval}")
    private String maxInterval;


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public ConsumerFactory<String, String> consumerFactory(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> orderFulfillmentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(orderFulfillmentGroupId));

        factory.setRetryTemplate(getRetryTemplateWithExpBOP());

        factory.setRecoveryCallback(new RecoveryCallback<Object>() {
            @Override
            public Object recover(RetryContext retryContext) throws Exception {
                ConsumerRecord message = (ConsumerRecord) retryContext.getAttribute(RetryingMessageListenerAdapter.CONTEXT_RECORD);
                logger.info("recover callback for message " + message.value());
                kafkaTemplate.send(topicDltName, message.value().toString());

                return null;
            }
        });
        return factory;
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> orderFulfillmentDTLKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(orderFulfillmentDTLGroupId));
        return factory;
    }

    @Bean
    public RetryTemplate getRetryTemplateWithExpBOP() {
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(Integer.valueOf(maxAttempts));

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(Integer.valueOf(initialInterval));
        backOffPolicy.setMultiplier(Double.valueOf(multiplier));
        backOffPolicy.setMaxInterval(Integer.valueOf(maxInterval));

        RetryTemplate aRetryTemplate = new RetryTemplate();
        aRetryTemplate.setRetryPolicy(simpleRetryPolicy);
        aRetryTemplate.setBackOffPolicy(backOffPolicy);

        return aRetryTemplate;
    }



    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("filter"));
        factory.setRecordFilterStrategy(record -> record.value()
            .contains("World"));
        return factory;
    }

    /*public ConsumerFactory<String, Greeting> greetingConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "greeting");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Greeting.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Greeting> greetingKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Greeting> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(greetingConsumerFactory());
        return factory;
    }*/

}
