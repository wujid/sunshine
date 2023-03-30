package com.sunshine.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * @author wjd
 * @description
 * @date 2022-07-05
 */
@Service
@Slf4j
public class ReceiverMessageService {


    @KafkaListener(id = "receiver1", topics = "${kafka.topic.topic-name}", groupId = "${kafka.topic.group-id}")
    public void listen(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("receiver1--我是消费者二,我消费了消息{}--分区为{}", message, record.partition());
            // 手动提交offset
            ack.acknowledge();
        }
    }

    @KafkaListener(id = "receiver2",topics = "${kafka.topic.topic-name}", groupId = "${kafka.topic.group-id}")
    public void listen2(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("receiver2--我是消费者二,我消费了消息{}--分区为{}", message, record.partition());
            // 手动提交offset
            ack.acknowledge();
        }
    }
}
