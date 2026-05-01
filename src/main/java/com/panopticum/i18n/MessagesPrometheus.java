package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MessagesPrometheus {

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("prometheus.metricsTitle", "Metrics"),
            Map.entry("prometheus.queryTitle", "Query"),
            Map.entry("prometheus.metric", "Metric"),
            Map.entry("prometheus.job", "Job"),
            Map.entry("prometheus.query", "PromQL Query"),
            Map.entry("prometheus.instant", "Instant"),
            Map.entry("prometheus.range", "Range"),
            Map.entry("prometheus.start", "Start"),
            Map.entry("prometheus.end", "End"),
            Map.entry("prometheus.step", "Step"),
            Map.entry("prometheus.execute", "Execute"),
            Map.entry("prometheus.ts", "Timestamp"),
            Map.entry("prometheus.value", "Value"),
            Map.entry("prometheus.noJobs", "No jobs found."),
            Map.entry("prometheus.noMetrics", "No metrics found."),
            Map.entry("prometheus.noResults", "No results."),
            Map.entry("prometheus.access.error", "Could not connect to Prometheus / VictoriaMetrics."),
            Map.entry("prometheus.access.unauthorized", "Unauthorized. Check credentials."),
            Map.entry("settings.placeholderNamePrometheus", "e.g. Production Prometheus"),
            Map.entry("settings.prometheusHost", "Host"),
            Map.entry("settings.placeholderPrometheusHost", "prometheus.example.com"),
            Map.entry("settings.prometheusAuth", "Auth (optional)"),
            Map.entry("settings.prometheusAuthHint", "Leave username empty to use password as Bearer token.")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("prometheus.metricsTitle", "Метрики"),
            Map.entry("prometheus.queryTitle", "Запрос"),
            Map.entry("prometheus.metric", "Метрика"),
            Map.entry("prometheus.job", "Job"),
            Map.entry("prometheus.query", "PromQL-запрос"),
            Map.entry("prometheus.instant", "Мгновенный"),
            Map.entry("prometheus.range", "Диапазон"),
            Map.entry("prometheus.start", "Начало"),
            Map.entry("prometheus.end", "Конец"),
            Map.entry("prometheus.step", "Шаг"),
            Map.entry("prometheus.execute", "Выполнить"),
            Map.entry("prometheus.ts", "Временная метка"),
            Map.entry("prometheus.value", "Значение"),
            Map.entry("prometheus.noJobs", "Job-ы не найдены."),
            Map.entry("prometheus.noMetrics", "Метрики не найдены."),
            Map.entry("prometheus.noResults", "Нет результатов."),
            Map.entry("prometheus.access.error", "Не удалось подключиться к Prometheus / VictoriaMetrics."),
            Map.entry("prometheus.access.unauthorized", "Не авторизован. Проверьте учётные данные."),
            Map.entry("settings.placeholderNamePrometheus", "Например: Production Prometheus"),
            Map.entry("settings.prometheusHost", "Хост"),
            Map.entry("settings.placeholderPrometheusHost", "prometheus.example.com"),
            Map.entry("settings.prometheusAuth", "Аутентификация (опционально)"),
            Map.entry("settings.prometheusAuthHint", "Оставьте логин пустым, чтобы использовать пароль как Bearer-токен.")
    );
}
