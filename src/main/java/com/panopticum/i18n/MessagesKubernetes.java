package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MessagesKubernetes {

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("kubernetes.namespacesTitle", "Namespaces"),
            Map.entry("kubernetes.podsTitle", "Pods"),
            Map.entry("kubernetes.logsTitle", "Logs"),
            Map.entry("kubernetes.namespace", "Namespace"),
            Map.entry("kubernetes.pod", "Pod"),
            Map.entry("kubernetes.phase", "Phase"),
            Map.entry("kubernetes.line", "Line"),
            Map.entry("kubernetes.message", "Message"),
            Map.entry("kubernetes.tailLines", "Tail lines"),
            Map.entry("kubernetes.noNamespaces", "No namespaces configured or connection error."),
            Map.entry("kubernetes.noPods", "No pods or connection error."),
            Map.entry("kubernetes.noLogs", "No log lines."),
            Map.entry("kubernetes.namespaceNotInConnection", "Namespace is not in this connection.")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("kubernetes.namespacesTitle", "Namespace"),
            Map.entry("kubernetes.podsTitle", "Pod-ы"),
            Map.entry("kubernetes.logsTitle", "Логи"),
            Map.entry("kubernetes.namespace", "Namespace"),
            Map.entry("kubernetes.pod", "Pod"),
            Map.entry("kubernetes.phase", "Фаза"),
            Map.entry("kubernetes.line", "Строка"),
            Map.entry("kubernetes.message", "Сообщение"),
            Map.entry("kubernetes.tailLines", "Последние строк"),
            Map.entry("kubernetes.noNamespaces", "Нет namespace или ошибка подключения."),
            Map.entry("kubernetes.noPods", "Нет pod-ов или ошибка подключения."),
            Map.entry("kubernetes.noLogs", "Нет строк лога."),
            Map.entry("kubernetes.namespaceNotInConnection", "Namespace не входит в это подключение.")
    );
}
