package com.panopticum.core.config;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                } catch (NumberFormatException ignored) {
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

    private static String decode(String s) {
        if (s == null) {
            return null;
        }
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    public record JdbcUrlParts(String type, String host, int port, String database, String username, String password) {
    }
}
