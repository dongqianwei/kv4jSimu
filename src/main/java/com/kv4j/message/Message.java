package com.kv4j.message;

public interface Message {
    Message setFromAddress(String message);
    Message setToAddress(String message);
    String getFromAddress();
    String getToAddress();
}
