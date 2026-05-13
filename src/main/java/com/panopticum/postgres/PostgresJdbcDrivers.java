package com.panopticum.postgres;

public final class PostgresJdbcDrivers {

    private PostgresJdbcDrivers() {
    }

    public static void ensureLoaded() {
        Holder.touch();
    }

    private static final class Holder {

        static {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        static void touch() {
        }
    }
}
