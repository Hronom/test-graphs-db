package com.github.hronom.test.graphs.db.base;

import java.nio.file.Path;

public interface TripleDatabaseModel {
    boolean openForBulkLoading();
    boolean bulkLoad(Path sourcePath);
    boolean closeAfterBulkLoading();

    boolean openForInsert();
    boolean insert(String tagNameA, String tagNameB);
    boolean closeAfterInsert();

    boolean openForIsRelated();
    boolean isRelated(String tagNameA, String tagNameB);
    boolean closeAfterIsRelated();

    boolean openForReadingAllProperties();
    boolean readAllProperties(String tagNameA);
    boolean closeAfterReadingAllProperties();
}
