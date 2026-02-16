package com.panopticum.core.config;

import lombok.extern.slf4j.Slf4j;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public final class JdbcUrlParser {

    private static final Pattern JDBC_PREFIX = Pattern.compile("^jdbc:([^:]+):(?://)?(.*)$");

    private JdbcUrlParser() {
    }

    public static JdbcUrlParts parse(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return null;
        }

        Matcher m = JDBC_PREFIX.matcher(jdbcUrl.trim());
        if (!m.matches()) {
            return null;
        }

        String type = m.group(1).toLowerCase();
        String rest = m.group(2);

        if ("sqlserver".equals(type)) {
            return parseSqlServerUrl(rest);
        }
        if ("oracle".equals(type)) {
            return parseOracleUrl(rest);
        }

        String authority = rest;
        String path = "";
        String query = "";

        int pathStart = rest.indexOf('/');
        if (pathStart >= 0) {
            authority = rest.substring(0, pathStart);
            String pathAndQuery = rest.substring(pathStart + 1);
            int q = pathAndQuery.indexOf('?');
            if (q >= 0) {
                path = pathAndQuery.substring(0, q);
                query = pathAndQuery.substring(q + 1);
            } else {
                path = pathAndQuery;
            }
        }

        String database = path.isBlank() ? "" : path;

        String username = null;
        String password = null;
        String host = "localhost";
        int port = -1;

        int at = authority.lastIndexOf('@');
        if (at >= 0) {
            String userinfo = authority.substring(0, at);
            authority = authority.substring(at + 1);
            int colon = userinfo.indexOf(':');
            if (colon >= 0) {
                username = decode(userinfo.substring(0, colon));
                password = decode(userinfo.substring(colon + 1));
            } else {
                username = decode(userinfo);
            }
        }

        if (!authority.isBlank()) {
            int colon = authority.indexOf(':');
            if (colon >= 0) {
                host = authority.substring(0, colon);
                try {
                    port = Integer.parseInt(authority.substring(colon + 1));
                } catch (NumberFormatException e) {
                    log.warn("Cannot parse port from JDBC URL authority: '{}'", authority);
                }
            } else {
                host = authority;
            }
        }

        if (!query.isBlank()) {
            for (String param : query.split("&")) {
                int eq = param.indexOf('=');
                if (eq > 0) {
                    String key = param.substring(0, eq).toLowerCase();
                    String value = decode(param.substring(eq + 1));
                    if ("user".equals(key) && username == null) {
                        username = value;
                    } else if ("password".equals(key) && password == null) {
                        password = value;
                    }
                }
            }
        }

        return new JdbcUrlParts(type, host, port, database, username, password);
    }

    private static JdbcUrlParts parseSqlServerUrl(String rest) {
        String host = "localhost";
        int port = -1;
        String database = "";
        String username = null;
        String password = null;

        String[] segments = rest.split(";");
        if (segments.length > 0 && !segments[0].isBlank()) {
            String first = segments[0].trim();
            int colon = first.indexOf(':');
            if (colon >= 0) {
                host = first.substring(0, colon).trim();
                if (host.isBlank()) {
                    host = "localhost";
                }
                try {
                    port = Integer.parseInt(first.substring(colon + 1).trim());
                } catch (NumberFormatException e) {
                    log.warn("Cannot parse SQL Server port from: '{}'", first);
                }
            } else {
                host = first.isBlank() ? "localhost" : first;
            }
        }

        for (int i = 1; i < segments.length; i++) {
            String seg = segments[i].trim();
            int eq = seg.indexOf('=');
            if (eq > 0) {
                String key = seg.substring(0, eq).trim().toLowerCase();
                String value = decode(seg.substring(eq + 1).trim());
                switch (key) {
                    case "databasename", "database" -> database = value != null ? value : "";
                    case "user" -> username = value;
                    case "password" -> password = value;
                    default -> { }
                }
            }
        }

        return new JdbcUrlParts("sqlserver", host, port, database, username, password);
    }

    private static JdbcUrlParts parseOracleUrl(String rest) {
        String host = "localhost";
        int port = -1;
        String database = "";
        String username = null;
        String password = null;

        String trimmed = rest.trim();
        if (trimmed.startsWith("//")) {
            String withoutSlash = trimmed.substring(2);
            int slash = withoutSlash.indexOf('/');
            if (slash >= 0) {
                database = withoutSlash.substring(slash + 1).trim();
                String hostPort = withoutSlash.substring(0, slash).trim();
                int colon = hostPort.indexOf(':');
                if (colon >= 0) {
                    host = hostPort.substring(0, colon).trim();
                    if (host.isBlank()) {
                        host = "localhost";
                    }
                    try {
                        port = Integer.parseInt(hostPort.substring(colon + 1).trim());
                    } catch (NumberFormatException e) {
                        log.warn("Cannot parse Oracle port from: '{}'", hostPort);
                    }
                } else {
                    host = hostPort.isBlank() ? "localhost" : hostPort;
                }
            }
        } else {
            String[] parts = trimmed.split(":");
            if (parts.length >= 1 && !parts[0].isBlank()) {
                host = parts[0].trim();
            }
            if (parts.length >= 2) {
                try {
                    port = Integer.parseInt(parts[1].trim());
                } catch (NumberFormatException e) {
                    log.warn("Cannot parse Oracle port from: '{}'", parts[1].trim());
                }
            }
            if (parts.length >= 3 && !parts[2].isBlank()) {
                database = parts[2].trim();
            }
        }

        return new JdbcUrlParts("oracle", host, port > 0 ? port : 1521, database, username, password);
    }

    private static String decode(String s) {
        if (s == null) {
            return null;
        }
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Failed to URL-decode '{}': {}", s, e.getMessage());
            return s;
        }
    }

    public record JdbcUrlParts(String type, String host, int port, String database, String username, String password) {
    }
}
