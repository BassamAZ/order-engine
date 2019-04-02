package com.usp.engine;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class Consumer {

    private static final Logger logger= LoggerFactory.getLogger(Consumer.class);

    @Autowired
    KafkaTemplate<String, String > kafkaTemplate;

    @Value("${order-submission.topic-name}")
    private String topicName;


    @KafkaListener(concurrency = "${order-submission.num-threads}",topics = "${order-submission.topic-name}",groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message, Acknowledgment acknowledgment){

        logger.info(String.format("**Main** consume started for message %s",message));

        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (message.equalsIgnoreCase("fail")){
            logger.info(String.format("**Main** consume FAILED for message %s",message));

            acknowledgment.acknowledge();
            throw new RuntimeException("consume FAILED for message "+message);
        }

        acknowledgment.acknowledge();
        logger.info(String.format("**Main** consume finished for message %s",message));
    }

    /*@KafkaListener(concurrency = "${order-submission.num-threads}",topics = "${order-submission.topic-name-dlt}",groupId = "${order-submission.group-id-dlt}")
    public void consumeDlt(String message, Acknowledgment acknowledgment){

        logger.info(String.format("**DLT** consume started for message %s",message));

        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        message="success";
        kafkaTemplate.send(topicName, message);

        acknowledgment.acknowledge();
        logger.info(String.format("**DLT** consume finished for message %s",message));

    }
*/

}
