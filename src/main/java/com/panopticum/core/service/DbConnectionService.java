package com.panopticum.core.service;

import com.panopticum.core.audit.AuditService;
import com.panopticum.core.model.ConnectionType;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.repository.DbConnectionRepository;
import com.panopticum.core.util.BreadcrumbPathHelper;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Singleton
@RequiredArgsConstructor
public class DbConnectionService {

    private final DbConnectionRepository repository;
    private final AuditService auditService;

    public List<DbConnection> findAll() {
        return repository.findAll();
    }

    public Optional<DbConnection> findById(Long id) {
        return repository.findById(id);
    }

    public Optional<DbConnection> findByName(String name) {
        return repository.findByName(name);
    }

    public Optional<BreadcrumbPathHelper.BreadcrumbMatch> findByBreadcrumbCopyPath(String copyPath) {
        return BreadcrumbPathHelper.matchLongestConnectionName(copyPath, repository.findAll());
    }

    public DbConnection save(DbConnection connection) {
        validateName(connection);
        boolean create = connection.getId() == null;
        DbConnection saved = repository.save(connection);
        if (create) {
            auditService.connectionCreate(saved.getId(), saved.getType(), saved.getName());
        } else {
            auditService.connectionUpdate(saved.getId(), saved.getType(), saved.getName());
        }

        return saved;
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
        repository.findById(id).ifPresent(conn ->
                auditService.connectionDelete(conn.getId(), conn.getType(), conn.getName()));
        repository.deleteById(id);
    }

    public List<String> listConfiguredUiPaths() {
        List<String> paths = new ArrayList<>();
        for (DbConnection conn : repository.findAll()) {
            Optional<ConnectionType> type = ConnectionType.fromStoredType(conn.getType());
            if (type.isEmpty() || conn.getId() == null) {
                continue;
            }
            String prefix = type.get().getUiPathPrefix();
            paths.add(prefix + "/" + conn.getId());
            String dbName = conn.getDbName();
            if (dbName != null && !dbName.isBlank()) {
                paths.add(prefix + "/" + conn.getId() + "/" + encodeUiPathSegment(dbName));
            }
        }
        paths.sort(Comparator.naturalOrder());
        return paths;
    }

    public static String encodeUiPathSegment(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
