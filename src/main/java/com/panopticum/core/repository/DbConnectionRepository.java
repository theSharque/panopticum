package com.panopticum.core.repository;

import com.panopticum.core.model.DbConnection;
import jakarta.inject.Singleton;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class DbConnectionRepository {

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS db_connections ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
                    + "name VARCHAR(255) NOT NULL, "
                    + "type VARCHAR(50) NOT NULL DEFAULT 'postgresql', "
                    + "host VARCHAR(255) NOT NULL, "
                    + "port INT NOT NULL DEFAULT 5432, "
                    + "db_name VARCHAR(255) NOT NULL, "
                    + "username VARCHAR(255) NOT NULL, "
                    + "password VARCHAR(255), "
                    + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"
                    + ")";

    private final DataSource dataSource;
    private final AtomicBoolean tableChecked = new AtomicBoolean(false);

    public DbConnectionRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private void ensureTableExists() {
        if (!tableChecked.compareAndSet(false, true)) {
            return;
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_SQL);
        } catch (SQLException e) {
            tableChecked.set(false);
            throw new RuntimeException("Failed to ensure db_connections table", e);
        }
    }

    public List<DbConnection> findAll() {
        ensureTableExists();
        String sql = "SELECT id, name, type, host, port, db_name, username, password, created_at FROM db_connections ORDER BY name LIMIT 500";
        List<DbConnection> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                result.add(mapRow(rs));
            }
        } catch (SQLException e) {
            return List.of();
        }

        return result;
    }

    public Optional<DbConnection> findById(Long id) {
        ensureTableExists();

        String sql = "SELECT id, name, type, host, port, db_name, username, password, created_at FROM db_connections WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find connection", e);
        }

        return Optional.empty();
    }

    public DbConnection save(DbConnection conn) {
        if (conn.getId() == null) {
            return insert(conn);
        }

        update(conn);

        return conn;
    }

    public void deleteById(Long id) {
        String sql = "DELETE FROM db_connections WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete connection", e);
        }
    }

    private DbConnection insert(DbConnection c) {
        ensureTableExists();

        String sql = "INSERT INTO db_connections (name, type, host, port, db_name, username, password) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, c.getName());
            stmt.setString(2, c.getType() != null ? c.getType() : "postgresql");
            stmt.setString(3, c.getHost());
            stmt.setInt(4, c.getPort());
            stmt.setString(5, c.getDbName());
            stmt.setString(6, c.getUsername());
            stmt.setString(7, c.getPassword());
            stmt.executeUpdate();

            try (ResultSet keys = stmt.getGeneratedKeys()) {
                if (keys.next()) {
                    c.setId(keys.getLong(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert connection", e);
        }

        return c;
    }

    private void update(DbConnection c) {
        String sql = "UPDATE db_connections SET name = ?, type = ?, host = ?, port = ?, db_name = ?, username = ?, password = ? WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, c.getName());
            stmt.setString(2, c.getType() != null ? c.getType() : "postgresql");
            stmt.setString(3, c.getHost());
            stmt.setInt(4, c.getPort());
            stmt.setString(5, c.getDbName());
            stmt.setString(6, c.getUsername());
            stmt.setString(7, c.getPassword());
            stmt.setLong(8, c.getId());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update connection", e);
        }
    }

    private static DbConnection mapRow(ResultSet rs) throws SQLException {
        return DbConnection.builder()
                .id(rs.getLong("id"))
                .name(rs.getString("name"))
                .type(rs.getString("type"))
                .host(rs.getString("host"))
                .port(rs.getInt("port"))
                .dbName(rs.getString("db_name"))
                .username(rs.getString("username"))
                .password(rs.getString("password"))
                .createdAt(rs.getString("created_at"))
                .build();
    }
}
