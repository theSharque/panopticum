package com.panopticum.couchbase.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Serdeable
public class CouchbaseScopeCollections {

    private String scopeName;
    private List<String> collectionNames;
}
