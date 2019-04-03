package com.usp.service;

import java.util.Map;

public interface OrderService {

    Map<String, String> submit(String orderId);

    void submitRetryStart();

    void submitRetryStop();
}
