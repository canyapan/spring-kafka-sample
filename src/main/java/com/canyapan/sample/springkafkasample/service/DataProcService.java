package com.canyapan.sample.springkafkasample.service;

import org.springframework.stereotype.Service;

@Service
public class DataProcService {

    public void process(String key, String message) {
        try {
            Thread.sleep(500);
        } catch (InterruptedException interrupt) {
            Thread.currentThread().interrupt();
        }
    }
}
