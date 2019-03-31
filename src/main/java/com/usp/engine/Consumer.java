package com.usp.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class Consumer {

    private Logger logger= LoggerFactory.getLogger(Consumer.class);


    @KafkaListener(concurrency = "${order-submission.num-threads}",topics = "${order-submission.topic-name}",groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message){

        logger.info(String.format("consume started for message %s",message));

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info(String.format("consume finished for message %s",message));
    }


}
