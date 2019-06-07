package com.huang.webflux.amqp.config;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author hsz
 */

@Component
public class MessageListenerContainerFactory {

    private final ConnectionFactory connectionFactory;

    @Autowired
    public MessageListenerContainerFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public MessageListenerContainer createMessageListenerContainer(String queueName) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.addQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        return container;
    }
}