package com.github.hronom.test.graphs.db.base.models;

import java.nio.file.Path;

public interface QuadDatabaseTestModel {
    boolean initialize();

    boolean openForBulkLoading();
    boolean bulkInsert(String graph, Path sourcePath);
    boolean closeAfterBulkLoading();

    boolean insert(String tagNameA, String tagNameB, String graph);
    boolean isRelated(String tagNameA, String tagNameB, String graph);
    boolean readAllProperties(String tagNameA, String graph);

    boolean copyToGraph(String graphA, String graphB);

    boolean commit();
}
