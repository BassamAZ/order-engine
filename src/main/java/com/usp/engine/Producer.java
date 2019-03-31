package com.usp.engine;

import com.usp.common.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class Producer {

    @Value("${order-submission.topic-name}")
    private String topicName;

    private Logger logger= LoggerFactory.getLogger(Producer.class);

    @Autowired
    KafkaTemplate<String, String > kafkaTemplate;

    public Map<String,String> publish(String orderId){
        logger.info(String.format("publish started for orderId %s",orderId));

        kafkaTemplate.send(topicName,orderId);

        Map<String,String> result= new HashMap<String,String>();
        result.put("status", OrderStatus.SUCCESS.getValue());

        logger.info(String.format("publish finished for orderId %s",orderId));

        return result;

    }





}


