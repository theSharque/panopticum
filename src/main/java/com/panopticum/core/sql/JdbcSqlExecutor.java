package com.panopticum.core.sql;

import com.panopticum.core.model.QueryResultData;
import lombok.experimental.UtilityClass;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class JdbcSqlExecutor {

    public static final String ROWS_AFFECTED_COLUMN = "rows_affected";

    public static QueryResultData execute(Statement stmt, String sql) throws SQLException {
        SqlStatementKind kind = SqlStatementClassifier.kindOf(sql);

        if (kind == SqlStatementKind.SELECT) {
            if (SqlStatementClassifier.hasReturning(sql)) {
                return readViaExecute(stmt, sql);
            }

            return readResultSet(stmt.executeQuery(sql));
        }

        if (kind == SqlStatementKind.MUTATION) {
            return rowsAffected(stmt.executeUpdate(sql));
        }

        return rowsAffected(executeOther(stmt, sql));
    }

    public static QueryResultData execute(PreparedStatement ps, String sql) throws SQLException {
        SqlStatementKind kind = SqlStatementClassifier.kindOf(sql);

        if (kind == SqlStatementKind.SELECT) {
            if (SqlStatementClassifier.hasReturning(sql)) {
                return readViaExecute(ps);
            }

            try (ResultSet rs = ps.executeQuery()) {
                return readResultSet(rs);
            }
        }

        if (kind == SqlStatementKind.MUTATION) {
            return rowsAffected(ps.executeUpdate());
        }

        return rowsAffected(executeOther(ps));
    }

    private static QueryResultData readViaExecute(Statement stmt, String sql) throws SQLException {
        if (!stmt.execute(sql)) {
            return rowsAffected(stmt.getUpdateCount());
        }

        try (ResultSet rs = stmt.getResultSet()) {
            return readResultSet(rs);
        }
    }

    private static QueryResultData readViaExecute(PreparedStatement ps) throws SQLException {
        if (!ps.execute()) {
            return rowsAffected(ps.getUpdateCount());
        }

        try (ResultSet rs = ps.getResultSet()) {
            return readResultSet(rs);
        }
    }

    public static QueryResultData rowsAffected(int count) {
        return new QueryResultData(
                List.of(ROWS_AFFECTED_COLUMN),
                List.of("integer"),
                List.of(List.of(count)));
    }

    private static int executeOther(Statement stmt, String sql) throws SQLException {
        boolean hasResultSet = stmt.execute(sql);
        if (hasResultSet) {
            try (ResultSet rs = stmt.getResultSet()) {
                readResultSet(rs);
            }
        }

        return stmt.getUpdateCount();
    }

    private static int executeOther(PreparedStatement ps) throws SQLException {
        boolean hasResultSet = ps.execute();
        if (hasResultSet) {
            try (ResultSet rs = ps.getResultSet()) {
                readResultSet(rs);
            }
        }

        return ps.getUpdateCount();
    }

    public static QueryResultData readResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        List<String> columns = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();

        for (int i = 1; i <= colCount; i++) {
            columns.add(meta.getColumnLabel(i));
            String typeName = meta.getColumnTypeName(i);
            int nullable = meta.isNullable(i);
            String nullability = nullable == ResultSetMetaData.columnNoNulls ? " NOT NULL"
                    : (nullable == ResultSetMetaData.columnNullable ? " NULL" : "");
            columnTypes.add(typeName + nullability);
        }

        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= colCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }

        return new QueryResultData(columns, columnTypes, rows);
    }
}
