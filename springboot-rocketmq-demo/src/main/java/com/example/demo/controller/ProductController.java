package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
public class ProductController {

    @Autowired
    private DefaultMQProducer defaultMQProducer;

    /**
     * @Description 发送延迟消息
     * @author liuhu
     * @param
     * @date 2022/1/18 15:30
     * @return void
     */
    @GetMapping("delay")
    public void delayMessage() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message("data_sync_topic", "hello!".getBytes());
        message.setDelayTimeLevel(5);
        defaultMQProducer.send(message);
    }

    /**
     * @Description 发送顺序消息
     * @author liuhu
     * @param 
     * @date 2022/1/18 15:30
     * @return void
     */
    @GetMapping("order")
    private void orderMessage() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message("data_sync_topic", "hello + orderId:12344!".getBytes());
        // MessageQueueSelector定义一定的算法将同类消息发送到指定的queue，类似保证同一个订单号的商品在队列有序
        SendResult sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object arg) {
                int value = arg.hashCode();
                if (value < 0) {
                    value = Math.abs(value);
                }

                value = value % list.size();
                return list.get(value);
            }
        }, "12344");
        log.info(JSON.toJSONString(sendResult));
    }

    /**
     * @Description 同步发送
     * @author liuhu
     * @param
     * @date 2022/1/18 15:50
     * @return void
     */
    @GetMapping("send")
    public void sead() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message("data_sync_topic", "hello!".getBytes());
        defaultMQProducer.send(message);
    }

    /**
     * @Description 异步发送
     * @author liuhu
     * @param
     * @date 2022/1/18 15:50
     * @return void
     */
    @GetMapping("asyncSead")
    public void asyncSead() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message("data_sync_topic", "hello!".getBytes());
        defaultMQProducer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("send success");
            }

            @Override
            public void onException(Throwable throwable) {

            }
        });
    }

}
