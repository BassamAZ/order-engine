package com.usp.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${order-submission.topic-name}")
    private String topicName;


    @KafkaListener( containerFactory = "orderFulfillmentKafkaListenerContainerFactory",concurrency = "${order-submission.num-threads}", topics = "${order-submission.topic-name}", groupId = "${order-submission.group-id}")
    public void consume(String message) {

        logger.info(String.format("**Main** consume started for message %s", message));

        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (message.equalsIgnoreCase("fail")) {
            logger.info(String.format("**Main** consume FAILED for message %s", message));

            throw new RuntimeException("consume FAILED for message " + message);
        }

        logger.info(String.format("**Main** consume finished for message %s", message));
    }

    @KafkaListener(containerFactory = "orderFulfillmentDTLKafkaListenerContainerFactory",id = "consumeDlt",autoStartup = "false", concurrency = "${order-submission.num-threads}", topics = "${order-submission.topic-name-dlt}", groupId = "${order-submission.group-id-dlt}")
    public void consumeDlt(String message) {

        logger.info(String.format("**DLT** consume started for message %s", message));

        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        message = "success";
        kafkaTemplate.send(topicName, message);

        //acknowledgment.acknowledge();
        logger.info(String.format("**DLT** consume finished for message %s", message));

    }

}
