package com.usp.service;

import com.usp.engine.Consumer;
import com.usp.engine.DltConsumer;
import com.usp.engine.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    private final Producer producer;

    @Autowired
    private  DltConsumer dltConsumer;

    @Autowired
    private Consumer consumerDlt;

    @Autowired
    private KafkaListenerEndpointRegistry registry;


    @Autowired
    OrderServiceImpl(Producer producer) {
        this.producer = producer;
    }

    @Override
    public Map<String, String> submit(String orderId) {
        return producer.publish(orderId);
    }

    @Override
    public void submitRetryStart() {
        registry.getListenerContainer("consumeDlt").start();
    }

    @Override
    public void submitRetryStop() {
        registry.getListenerContainer("consumeDlt").stop();
    }
}
