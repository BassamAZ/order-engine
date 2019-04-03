package com.usp.service;

import com.usp.engine.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    private final Producer producer;
    private final KafkaListenerEndpointRegistry registry;


    @Autowired
    OrderServiceImpl(Producer producer, KafkaListenerEndpointRegistry registry) {
        this.producer = producer;
        this.registry = registry;
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
