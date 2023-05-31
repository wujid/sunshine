package com.sunshine.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sunshine.model.Messages;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Date;

/**
 * @author wjd
 * @description
 * @date 2022-07-05
 */
@Service
@Slf4j
public class SenderMessageService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.topic-name}")
    private String topic;

    @Autowired(required = false)
    Producer<String, Object> producer;

    private Gson gson = new GsonBuilder().create();

    public void send() {
        Messages message = new Messages();
        message.setId(System.currentTimeMillis());
        message.setMsg("222");
        message.setSendTime(new Date());
        kafkaTemplate.send(topic, gson.toJson(message));
    }

    public void sendMessage(String topic, Object object) {
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, object);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.info("发送消息失败:" + throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> sendResult) {
                log.info("发送消息成功:" + sendResult.toString());
            }
        });
    }

    public void sendRecord() throws Exception {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, "hello, Kafka ProducerRecord!");
        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + ":" + metadata.offset());
                    }
                }
            });
        } catch (Exception e) {
        }
    }
}
