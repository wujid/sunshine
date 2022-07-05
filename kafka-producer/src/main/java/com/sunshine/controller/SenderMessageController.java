package com.sunshine.controller;

import com.sunshine.run.SendMessageRun;
import com.sunshine.service.SenderMessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wjd
 * @description
 * @date 2022-07-05
 */
@RestController
public class SenderMessageController {

    @Autowired
    private SenderMessageService senderMessageService;

    @Qualifier("threadPool")
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    @Value("${kafka.topic.topic-name}")
    private String topic;

    @GetMapping("/sendSimple")
    public void sendSimple() {
        senderMessageService.sendMessage(topic, "hello spring boot kafka");
    }

    @GetMapping("/sendObject")
    public void sendObject() {
        senderMessageService.send();
    }

    @GetMapping("/sendMultiMessage")
    public void sendMultiMessage() {
        for (int i = 0; i < 50; i++) {
            String message = "我是第" + i + "条消息";
            SendMessageRun sendMessageRun = new SendMessageRun(topic, message);
            threadPoolTaskExecutor.execute(sendMessageRun);
        }
    }
}
