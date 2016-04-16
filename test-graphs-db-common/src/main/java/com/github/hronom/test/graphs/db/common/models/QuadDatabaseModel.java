package com.github.hronom.test.graphs.db.common.models;

import java.nio.file.Path;

public interface QuadDatabaseModel extends AutoCloseable {
    boolean open();

    boolean bulkUpload(String graph, Path ntFilePath);
    boolean insert(String graph, String subject, String predicate, String object);
    boolean commitChanges();

    boolean contains(String graph, String subject, String predicate, String object);
}
