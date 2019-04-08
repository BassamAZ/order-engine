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

    private static final Logger logger= LoggerFactory.getLogger(Producer.class);

    @Value("${order-submission.topic-name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

      /* @Autowired
        private KafkaTemplate<String, Greeting> greetingKafkaTemplate;*/

    public Map<String,String> publish(String orderId){
        logger.info(String.format("publish started for orderId %s",orderId));

        kafkaTemplate.send(topicName,orderId);

        Map<String,String> result= new HashMap<String,String>();
        result.put("status", OrderStatus.SUCCESS.getValue());

        logger.info(String.format("publish finished for orderId %s",orderId));

        return result;

    }

      /*  public void sendMessageToPartion(String message, int partition) {
            kafkaTemplate.send(partionedTopicName, partition, message);
        }*/

/*
        public void sendGreetingMessage(Greeting greeting) {
            greetingKafkaTemplate.send(greetingTopicName, greeting);
        }
*/






}


