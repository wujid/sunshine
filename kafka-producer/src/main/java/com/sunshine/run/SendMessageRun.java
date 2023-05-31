package com.sunshine.run;

import com.sunshine.util.SpringContextUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author wjd
 * @description
 * @date 2022-07-05
 */
public class SendMessageRun implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(SendMessageRun.class);

    public String topic;

    public Object message;

    public SendMessageRun(String topic, Object message) {
        this.topic = topic;
        this.message = message;
    }

    @Override
    public void run() {
        final KafkaTemplate kafkaTemplate = SpringContextUtils.getBean(KafkaTemplate.class);
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("发送消息失败:消息内容为{}", message);
                logger.error("发送消息失败", throwable);
            }

            @Override
            public void onSuccess(SendResult<String, Object> sendResult) {
                logger.info("发送消息成功:" + sendResult.toString());
            }
        });
    }
}
