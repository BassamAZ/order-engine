package com.usp.controller;

import com.usp.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/order")
public class OrderController {

    private static final Logger logger = LoggerFactory.getLogger(OrderController.class);

    @Autowired
    OrderService orderService;

    @PostMapping("/submit")
    public Map<String, String> submit(@RequestBody Map<String, String> order) {

        logger.info(String.format("submit started for order id %s ", order.get("orderId")));

        Map<String, String> result = orderService.submit(order.get("orderId"));
        result.put("orderId", order.get("orderId"));
        result.put("id", order.get("id"));

        logger.info(String.format("submit finished for order id %s ", order.get("orderId")));

        return result;
    }

    @PostMapping("/submit-retry-start")
    public void submitRetryStart() {

        logger.info(String.format("submitRetryStart started"));

        orderService.submitRetryStart();

        logger.info(String.format("submitRetryStart finished"));

    }

    @PostMapping("/submit-retry-stop")
    public void submitRetryStop() {

        logger.info(String.format("submitRetryStop started"));

        orderService.submitRetryStop();

        logger.info(String.format("submitRetryStop finished"));
    }

}
