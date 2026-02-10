package com.panopticum.core.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.repository.DbConnectionRepository;
import com.panopticum.core.service.DbConnectionFactory;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
@RequiredArgsConstructor
public class BootstrapConnectionsRunner {

    private static final String ENV_CONNECTIONS_JSON = "PANOPTICUM_CONNECTIONS_JSON";
    private static final Logger LOG = LoggerFactory.getLogger(BootstrapConnectionsRunner.class);

    private static final TypeReference<List<BootstrapConnectionEntry>> LIST_ENTRY_TYPE =
            new TypeReference<>() {
            };

    private final DbConnectionService dbConnectionService;
    private final DbConnectionRepository dbConnectionRepository;
    private final DbConnectionFactory dbConnectionFactory;
    private final ObjectMapper objectMapper;

    @EventListener
    public void onStartup(StartupEvent event) {
        if (!dbConnectionService.findAll().isEmpty()) {
            return;
        }

        String json = System.getenv(ENV_CONNECTIONS_JSON);
        if (json == null || json.isBlank()) {
            return;
        }

        List<BootstrapConnectionEntry> entries;
        try {
            entries = objectMapper.readValue(json, LIST_ENTRY_TYPE);
        } catch (Exception e) {
            LOG.warn("Failed to parse {}: {}", ENV_CONNECTIONS_JSON, e.getMessage());
            return;
        }

        if (entries == null) {
            return;
        }

        for (BootstrapConnectionEntry entry : entries) {
            if (entry.getName() == null || entry.getName().isBlank()) {
                continue;
            }
            if (dbConnectionRepository.findByName(entry.getName()).isPresent()) {
                continue;
            }

            DbConnection conn = toDbConnection(entry);
            if (conn != null) {
                dbConnectionService.save(conn);
                LOG.info("Bootstrap added connection: name={}, type={}, host={}:{}",
                        conn.getName(), conn.getType(), conn.getHost(), conn.getPort());
            }
        }
    }

    private DbConnection toDbConnection(BootstrapConnectionEntry entry) {
        String name = entry.getName();
        String type;
        String host;
        Integer port;
        String database;
        String username;
        String password;

        if (entry.getJdbcUrl() != null && !entry.getJdbcUrl().isBlank()) {
            JdbcUrlParser.JdbcUrlParts parts = JdbcUrlParser.parse(entry.getJdbcUrl());
            if (parts == null) {
                LOG.warn("Could not parse jdbcUrl for connection name {}", name);
                return null;
            }
            type = parts.type();
            host = parts.host();
            port = parts.port() >= 0 ? parts.port() : null;
            database = parts.database();
            username = parts.username();
            password = parts.password();
        } else {
            type = entry.getType();
            host = entry.getHost();
            port = entry.getPort();
            database = entry.getDatabase();
            username = entry.getUsername();
            password = entry.getPassword();
        }

        return dbConnectionFactory.build(type, name, host, port, database, username, password);
    }
}
