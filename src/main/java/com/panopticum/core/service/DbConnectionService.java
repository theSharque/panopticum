package com.panopticum.core.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.repository.DbConnectionRepository;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

@Singleton
@RequiredArgsConstructor
public class DbConnectionService {

    private final DbConnectionRepository repository;

    public List<DbConnection> findAll() {
        return repository.findAll();
    }

    public Optional<DbConnection> findById(Long id) {
        return repository.findById(id);
    }

    public DbConnection save(DbConnection connection) {
        validateName(connection);
        return repository.save(connection);
    }

    private void validateName(DbConnection conn) {
        String name = conn.getName();
        if (name == null || name.endsWith("/")) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, "connection.nameTrailingSlash");
        }
        repository.findByName(conn.getName()).ifPresent(existing -> {
            if (!existing.getId().equals(conn.getId())) {
                throw new HttpStatusException(HttpStatus.BAD_REQUEST, "connection.nameDuplicate");
            }
        });
    }

    public void deleteById(Long id) {
        repository.deleteById(id);
    }
}
