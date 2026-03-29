package com.panopticum.core.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class BreadcrumbItem {

    private String label;
    private String url;
    private boolean includeInCopyPath = true;

    public BreadcrumbItem(String label, String url) {
        this.label = label;
        this.url = url;
        this.includeInCopyPath = true;
    }

    public BreadcrumbItem(String label, String url, boolean includeInCopyPath) {
        this.label = label;
        this.url = url;
        this.includeInCopyPath = includeInCopyPath;
    }
}
