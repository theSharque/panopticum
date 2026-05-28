package com.panopticum.clickhouse;

public final class ClickHouseJdbcDrivers {

    private ClickHouseJdbcDrivers() {
    }

    public static void ensureLoaded() {
        Holder.touch();
    }

    private static final class Holder {

        static {
            try {
                Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        static void touch() {
        }
    }
}
