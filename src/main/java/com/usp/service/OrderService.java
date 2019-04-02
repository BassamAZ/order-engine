package com.usp.service;

import java.util.Map;

public interface OrderService {

    public Map<String, String> submit(String orderId);

    public Map<String, String> submitRetry(String orderId);
}
