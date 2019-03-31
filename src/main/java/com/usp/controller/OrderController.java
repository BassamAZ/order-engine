package com.usp.controller;

import com.usp.common.OrderStatus;
import com.usp.engine.Producer;
import com.usp.service.OrderService;
import lombok.extern.log4j.Log4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping ("/order")
public class OrderController {

    Logger logger= LoggerFactory.getLogger(OrderController.class);

    @Autowired
    OrderService orderService;

    @PostMapping("/submit")
    public Map<String,String> submit(@RequestBody  Map<String, String> order){

        logger.info(String.format("submit started for order id %s ",order.get("orderId")));

        Map<String,String> result=orderService.submit(order.get("orderId"));
        result.put("orderId",order.get("orderId"));
        result.put("id",order.get("id"));

        logger.info(String.format("submit finished for order id %s ",order.get("orderId")));

        return result;
    }

}
