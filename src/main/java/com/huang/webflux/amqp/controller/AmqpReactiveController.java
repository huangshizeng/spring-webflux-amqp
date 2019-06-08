package com.huang.webflux.amqp.controller;

import com.huang.webflux.amqp.config.MessageListenerContainerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * @author hsz
 */

@RestController
@Slf4j
public class AmqpReactiveController {

    private final MessageListenerContainerFactory messageListenerContainerFactory;

    @Autowired
    public AmqpReactiveController(MessageListenerContainerFactory messageListenerContainerFactory) {
        this.messageListenerContainerFactory = messageListenerContainerFactory;
    }

    @GetMapping(value = "listener", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> receiveMessagesFromQueue(@RequestParam String queueName) {
        MessageListenerContainer container = messageListenerContainerFactory.createMessageListenerContainer(queueName);
        return Flux.create(emitter -> {
            log.info("开始监听, queue={}", queueName);
            container.setupMessageListener((ChannelAwareMessageListener) (message, channle) -> {
                if (emitter.isCancelled()) {
                    log.info("停止消费, queue={}", queueName);
                    container.stop();
                    return;
                }
                String payload = new String(message.getBody());
                emitter.next(payload);
                log.info("收到来自{}的消息：{}", queueName, payload);
                channle.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            });
            emitter.onRequest(v -> container.start());
            emitter.onDispose(() -> {
                log.info("停止消费: queue={}", queueName);
                container.stop();
            });
        });
    }
}
