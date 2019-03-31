package com.usp.common;

import lombok.Getter;

@Getter
public enum OrderStatus {

    SUCCESS ("Success"),
    FAILED ("Failed");

    private String value;

    OrderStatus(String value){
        this.value=value;
    }
}
