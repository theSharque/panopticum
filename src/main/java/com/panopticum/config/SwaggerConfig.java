package com.panopticum.config;

import com.panopticum.cassandra.controller.CassandraController;
import com.panopticum.clickhouse.controller.ClickHouseController;
import com.panopticum.core.controller.DiffController;
import com.panopticum.core.controller.HomeController;
import com.panopticum.core.controller.LoginController;
import com.panopticum.core.controller.SettingsController;
import com.panopticum.core.controller.ThemeController;
import com.panopticum.elasticsearch.controller.ElasticsearchController;
import com.panopticum.i18n.LocaleController;
import com.panopticum.kafka.controller.KafkaController;
import com.panopticum.mongo.controller.MongoController;
import com.panopticum.mssql.controller.MssqlController;
import com.panopticum.mysql.controller.MySqlController;
import com.panopticum.oracle.controller.OracleController;
import com.panopticum.postgres.controller.PgController;
import com.panopticum.rabbitmq.controller.RabbitMqController;
import com.panopticum.redis.controller.RedisController;
import io.micronaut.openapi.annotation.OpenAPIExclude;
import io.micronaut.openapi.annotation.OpenAPIInclude;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import jakarta.inject.Singleton;

@Singleton
@OpenAPIInclude(packages = "com.panopticum" )
@OpenAPIExclude(classes = {
        CassandraController.class,
        ClickHouseController.class,
        DiffController.class,
        ElasticsearchController.class,
        HomeController.class,
        KafkaController.class,
        LocaleController.class,
        LoginController.class,
        MongoController.class,
        MssqlController.class,
        MySqlController.class,
        OracleController.class,
        PgController.class,
        RabbitMqController.class,
        RedisController.class,
        SettingsController.class,
        ThemeController.class
})
@OpenAPIDefinition(
        info = @Info(
                title = "Panopticum API",
                version = "1.0",
                description = "REST API for DB operations (MongoDB, Redis, ClickHouse, PostgreSQL, etc.)"
        ),
        security = @SecurityRequirement(name = "basicAuth")
)
@SecurityScheme(
        name = "basicAuth",
        type = SecuritySchemeType.HTTP,
        scheme = "basic",
        description = "Basic authentication (admin/admin by default)"
)
public class SwaggerConfig {
}
