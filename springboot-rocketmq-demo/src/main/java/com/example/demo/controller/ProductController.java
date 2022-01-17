package com.example.demo.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class ProductController {

    @Autowired
    private DefaultMQProducer defaultMQProducer;

    @GetMapping
    public  void delayMessage() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message("data_sync_topic","hello!" .getBytes());
        message.setDelayTimeLevel(5);
          defaultMQProducer.send(message);
        }

}
