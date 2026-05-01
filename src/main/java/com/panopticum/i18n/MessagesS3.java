package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MessagesS3 {

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("s3.bucketsTitle", "Buckets"),
            Map.entry("s3.objectsTitle", "Objects"),
            Map.entry("s3.peekTitle", "Peek"),
            Map.entry("s3.bucket", "Bucket"),
            Map.entry("s3.key", "Key"),
            Map.entry("s3.size", "Size"),
            Map.entry("s3.lastModified", "Last Modified"),
            Map.entry("s3.contentType", "Content Type"),
            Map.entry("s3.etag", "ETag"),
            Map.entry("s3.format", "Format"),
            Map.entry("s3.headBytes", "Head bytes"),
            Map.entry("s3.peek", "Peek"),
            Map.entry("s3.noBuckets", "No buckets found or access denied."),
            Map.entry("s3.noObjects", "No objects found."),
            Map.entry("s3.access.forbidden", "Access denied. Check your access key and permissions."),
            Map.entry("s3.access.notFound", "Bucket or object not found."),
            Map.entry("s3.access.error", "Could not connect to S3/MinIO."),
            Map.entry("settings.placeholderNameS3", "e.g. Production S3"),
            Map.entry("settings.s3Endpoint", "Endpoint (host or URL)"),
            Map.entry("settings.placeholderS3Host", "s3.amazonaws.com or minio.example.com"),
            Map.entry("settings.s3Region", "Region (db name field)"),
            Map.entry("settings.placeholderS3Region", "us-east-1"),
            Map.entry("settings.s3AccessKey", "Access Key (username)"),
            Map.entry("settings.s3SecretKey", "Secret Key (password)")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("s3.bucketsTitle", "Бакеты"),
            Map.entry("s3.objectsTitle", "Объекты"),
            Map.entry("s3.peekTitle", "Просмотр"),
            Map.entry("s3.bucket", "Бакет"),
            Map.entry("s3.key", "Ключ"),
            Map.entry("s3.size", "Размер"),
            Map.entry("s3.lastModified", "Дата изменения"),
            Map.entry("s3.contentType", "Тип контента"),
            Map.entry("s3.etag", "ETag"),
            Map.entry("s3.format", "Формат"),
            Map.entry("s3.headBytes", "Байт для просмотра"),
            Map.entry("s3.peek", "Просмотр"),
            Map.entry("s3.noBuckets", "Бакеты не найдены или доступ запрещён."),
            Map.entry("s3.noObjects", "Объекты не найдены."),
            Map.entry("s3.access.forbidden", "Доступ запрещён. Проверьте ключ и права."),
            Map.entry("s3.access.notFound", "Бакет или объект не найден."),
            Map.entry("s3.access.error", "Не удалось подключиться к S3/MinIO."),
            Map.entry("settings.placeholderNameS3", "Например: Production S3"),
            Map.entry("settings.s3Endpoint", "Endpoint (хост или URL)"),
            Map.entry("settings.placeholderS3Host", "s3.amazonaws.com или minio.example.com"),
            Map.entry("settings.s3Region", "Регион (поле база данных)"),
            Map.entry("settings.placeholderS3Region", "us-east-1"),
            Map.entry("settings.s3AccessKey", "Access Key (логин)"),
            Map.entry("settings.s3SecretKey", "Secret Key (пароль)")
    );
}
