package com.panopticum.i18n;

import java.util.Map;

public final class MessagesRabbitmq {

    private MessagesRabbitmq() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("rabbitmq.queuesTitle", "Queues"),
            Map.entry("rabbitmq.messagesTitle", "Messages"),
            Map.entry("rabbitmq.messageDetailTitle", "Message"),
            Map.entry("rabbitmq.vhost", "VHost"),
            Map.entry("rabbitmq.queue", "Queue"),
            Map.entry("rabbitmq.messages", "Messages"),
            Map.entry("rabbitmq.messagesReady", "Ready"),
            Map.entry("rabbitmq.messagesUnack", "Unacked"),
            Map.entry("rabbitmq.consumers", "Consumers"),
            Map.entry("rabbitmq.noQueues", "No queues or connection error."),
            Map.entry("rabbitmq.noMessages", "No messages or connection error."),
            Map.entry("rabbitmq.backToMessages", "Back to messages"),
            Map.entry("rabbitmq.routingKey", "Routing key"),
            Map.entry("rabbitmq.payload", "Payload"),
            Map.entry("rabbitmq.properties", "Properties"),
            Map.entry("rabbitmq.refresh", "Refresh"),
            Map.entry("connectionTest.failed", "Connection failed.")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("rabbitmq.queuesTitle", "Очереди"),
            Map.entry("rabbitmq.messagesTitle", "Сообщения"),
            Map.entry("rabbitmq.messageDetailTitle", "Сообщение"),
            Map.entry("rabbitmq.vhost", "VHost"),
            Map.entry("rabbitmq.queue", "Очередь"),
            Map.entry("rabbitmq.messages", "Сообщения"),
            Map.entry("rabbitmq.messagesReady", "Готово"),
            Map.entry("rabbitmq.messagesUnack", "Без ack"),
            Map.entry("rabbitmq.consumers", "Подписчики"),
            Map.entry("rabbitmq.noQueues", "Нет очередей или ошибка подключения."),
            Map.entry("rabbitmq.noMessages", "Нет сообщений или ошибка подключения."),
            Map.entry("rabbitmq.backToMessages", "Назад к сообщениям"),
            Map.entry("rabbitmq.routingKey", "Routing key"),
            Map.entry("rabbitmq.payload", "Тело"),
            Map.entry("rabbitmq.properties", "Свойства"),
            Map.entry("rabbitmq.refresh", "Обновить"),
            Map.entry("connectionTest.failed", "Подключение не удалось.")
    );
}
