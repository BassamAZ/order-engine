package com.usp.service;

import com.usp.engine.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    private final Producer producer;

    @Autowired
    OrderServiceImpl(Producer producer) {
        this.producer = producer;
    }


    @Override
    public Map<String, String> submit(String orderId) {
        return producer.publish(orderId);
    }
}
