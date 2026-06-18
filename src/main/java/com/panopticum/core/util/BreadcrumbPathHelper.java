package com.panopticum.core.util;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@UtilityClass
public class BreadcrumbPathHelper {

    public String joinCopyPath(List<BreadcrumbItem> breadcrumbs) {
        if (breadcrumbs == null || breadcrumbs.isEmpty()) {
            return "";
        }

        return breadcrumbs.stream()
                .filter(BreadcrumbItem::isIncludeInCopyPath)
                .map(b -> b.getLabel() != null ? b.getLabel() : "")
                .collect(Collectors.joining("/"));
    }

    public Optional<BreadcrumbMatch> matchLongestConnectionName(String copyPath, List<DbConnection> connections) {
        if (copyPath == null || copyPath.isBlank()) {
            return Optional.empty();
        }

        String normalized = trimTrailingSlash(copyPath.trim());
        return connections.stream()
                .filter(c -> c.getName() != null && !c.getName().isBlank())
                .filter(c -> normalized.equals(c.getName()) || normalized.startsWith(c.getName() + "/"))
                .max(Comparator.comparingInt(c -> c.getName().length()))
                .map(conn -> {
                    String scopePart = normalized.equals(conn.getName())
                            ? ""
                            : normalized.substring(conn.getName().length() + 1);
                    List<String> scopeLabels = scopePart.isBlank() ? List.of() : splitScopeLabels(scopePart);

                    return new BreadcrumbMatch(conn, scopeLabels);
                });
    }

    private static String trimTrailingSlash(String path) {
        if (path.endsWith("/") && path.length() > 1) {
            return path.substring(0, path.length() - 1);
        }

        return path;
    }

    private static List<String> splitScopeLabels(String scopePart) {
        return Arrays.stream(scopePart.split("/"))
                .filter(s -> !s.isEmpty())
                .toList();
    }

    public record BreadcrumbMatch(DbConnection connection, List<String> scopeLabels) {
    }
}
