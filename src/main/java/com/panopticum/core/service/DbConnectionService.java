package com.panopticum.core.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.repository.DbConnectionRepository;
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
        return repository.save(connection);
    }

    public void deleteById(Long id) {
        repository.deleteById(id);
    }
}
