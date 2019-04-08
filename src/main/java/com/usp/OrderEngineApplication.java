package com.usp;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.policy.TimeoutRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class OrderEngineApplication {

    private static final Logger logger = LoggerFactory.getLogger(OrderEngineApplication.class);

    @Value("${order-submission.topic-name}")
    private String topicName;
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

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${order-submission.group-id}")
    private String orderFulfillmentGroupId;



    public static void main(String[] args) {
        SpringApplication.run(OrderEngineApplication.class, args);
    }

    private static RetryTemplate getRetryTemplateWithFixBOP(Integer timeoutInSec) {
        // we set the retry time out
        TimeoutRetryPolicy retryPolicy = new TimeoutRetryPolicy();
        retryPolicy.setTimeout(timeoutInSec * 1000 * 60);

        // we set the back off period to 60s
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(60000);

        // our retry service
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

    @Bean
    public NewTopic topic() {
        return new NewTopic(topicName, Integer.valueOf(numPartitions), Short.valueOf(numReplicas));
    }

    @Bean
    public NewTopic topicDlt() {
        return new NewTopic(topicDltName, Integer.valueOf(numPartitions), Short.valueOf(numReplicas));
    }

   /* @Bean
    public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setRetryTemplate(getRetryTemplateWithExpBOP());
        factory.setConsumerFactory(consumerFactory(orderFulfillmentGroupId));

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
    }*/

    /*@Bean
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
*/
    /**
     * Returns a retry template that always retries.  Starts with a second interval between retries and doubles that interval up
     * to a minute for each retry.
     *
     * @return A retry template.
     */
    protected RetryTemplate retryTemplateSimple() {
        RetryTemplate retry = new RetryTemplate();

        Map<Class<? extends Throwable>, Boolean> stopExceptions =
                Collections.singletonMap(InterruptedException.class, Boolean.FALSE);
        SimpleRetryPolicy retryPolicy =
                new SimpleRetryPolicy(Integer.MAX_VALUE, stopExceptions, true, true);

        retry.setRetryPolicy(retryPolicy);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(2000);
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(60000);
        retry.setBackOffPolicy(backOffPolicy);

        return retry;
    }

}
